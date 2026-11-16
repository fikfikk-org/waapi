package media

import (
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"image"
	"image/color"
	"image/jpeg"
	"image/png"
	"io"
	"os"
	"runtime"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/fikfikk-org/waapi/internal/journal"
	"github.com/fikfikk-org/waapi/internal/store"
	"go.mau.fi/whatsmeow"
)

type fakeUploader struct {
	fn func(ctx context.Context, plaintext io.Reader, tempFile io.ReadWriteSeeker, appInfo whatsmeow.MediaType) (whatsmeow.UploadResponse, error)
}

func (f fakeUploader) UploadReader(ctx context.Context, plaintext io.Reader, tempFile io.ReadWriteSeeker, appInfo whatsmeow.MediaType) (whatsmeow.UploadResponse, error) {
	return f.fn(ctx, plaintext, tempFile, appInfo)
}

type countingReader struct {
	data []byte
	pos  int
	read atomic.Int64
}

func (r *countingReader) Read(p []byte) (int, error) {
	if r.pos >= len(r.data) {
		return 0, io.EOF
	}
	n := copy(p, r.data[r.pos:])
	r.pos += n
	r.read.Add(int64(n))
	return n, nil
}

type failingMidReader struct {
	prefix []byte
	pos    int
	failAt int
	err    error
}

func (r *failingMidReader) Read(p []byte) (int, error) {
	if r.pos >= r.failAt {
		return 0, r.err
	}
	if len(r.prefix) > 0 && r.pos < len(r.prefix) {
		n := copy(p, r.prefix[r.pos:])
		r.pos += n
		return n, nil
	}
	remain := r.failAt - r.pos
	if remain > len(p) {
		remain = len(p)
	}
	for i := 0; i < remain; i++ {
		p[i] = 'A'
	}
	r.pos += remain
	return remain, nil
}

type syscallCall struct {
	op  string
	off int64
	n   int64
}

type fakeSyscalls struct {
	calls []syscallCall
}

func (f *fakeSyscalls) SyncRange(_ uintptr, off, n int64) error {
	f.calls = append(f.calls, syscallCall{op: "sync", off: off, n: n})
	return nil
}

func (f *fakeSyscalls) DontNeed(_ uintptr, off, n int64) error {
	f.calls = append(f.calls, syscallCall{op: "dontneed", off: off, n: n})
	return nil
}

func makePNGBytes(w, h int) []byte {
	img := image.NewRGBA(image.Rect(0, 0, w, h))
	img.Set(0, 0, color.RGBA{R: 255, A: 255})
	var buf bytes.Buffer
	_ = png.Encode(&buf, img)
	return buf.Bytes()
}

func makeJPEGBytes(w, h int) []byte {
	img := image.NewRGBA(image.Rect(0, 0, w, h))
	var buf bytes.Buffer
	_ = jpeg.Encode(&buf, img, &jpeg.Options{Quality: 75})
	return buf.Bytes()
}

func makeWebPVPXBytes(w, h uint32) []byte {
	// Bangun header RIFF WEBP dengan chunk VP8X minimal (ukuran payload 10 byte).
	var buf bytes.Buffer
	buf.WriteString("RIFF")
	_ = binary.Write(&buf, binary.LittleEndian, uint32(4+8+10))
	buf.WriteString("WEBP")
	buf.WriteString("VP8X")
	_ = binary.Write(&buf, binary.LittleEndian, uint32(10))
	buf.Write([]byte{0, 0, 0, 0}) // flags
	wm1 := w - 1
	hm1 := h - 1
	buf.Write([]byte{byte(wm1), byte(wm1 >> 8), byte(wm1 >> 16)})
	buf.Write([]byte{byte(hm1), byte(hm1 >> 8), byte(hm1 >> 16)})
	return buf.Bytes()
}

func makeOggOpusHeader() []byte {
	h := make([]byte, 40)
	copy(h[0:4], "OggS")
	copy(h[28:36], "OpusHead")
	return h
}

// TestUpload_SatuPassTanpaFilePlaintext memastikan Upload bekerja dalam satu pass tanpa file temp plaintext di direktori tmp.
func TestUpload_SatuPassTanpaFilePlaintext(t *testing.T) {
	tmp := t.TempDir()
	pipe := NewPipeline(Limits{
		MediaMaxBytes:    1 << 20,
		DocumentMaxBytes: 2 << 20,
		TmpDir:           tmp,
	})

	payload := makePNGBytes(120, 80)
	up := fakeUploader{
		fn: func(ctx context.Context, plaintext io.Reader, tempFile io.ReadWriteSeeker, appInfo whatsmeow.MediaType) (whatsmeow.UploadResponse, error) {
			if appInfo != whatsmeow.MediaImage {
				t.Fatalf("appInfo diharapkan MediaImage, didapat %q", appInfo)
			}
			// Salin ciphertext simulasi ke tempFile agar tempFile terisi.
			n, err := io.Copy(tempFile, plaintext)
			if err != nil {
				return whatsmeow.UploadResponse{}, err
			}
			entries, err := os.ReadDir(tmp)
			if err != nil {
				return whatsmeow.UploadResponse{}, err
			}
			if runtime.GOOS == "linux" {
				if len(entries) != 0 {
					t.Fatalf("direktori tmp pada Linux harus kosong selama UploadReader berjalan, ditemukan %d entri", len(entries))
				}
			} else if len(entries) > 1 {
				t.Fatalf("tidak boleh ada file plaintext terpisah di tmp (maks 1 file temp ciphertext), ditemukan %d entri", len(entries))
			}
			return whatsmeow.UploadResponse{
				URL:        "https://mmg.whatsapp.net/v/test",
				DirectPath: "/v/test",
				FileLength: uint64(n),
				FileSHA256: []byte{1, 2, 3, 4},
				MediaKey:   make([]byte, 32),
			}, nil
		},
	}

	res, err := pipe.Upload(context.Background(), up, bytes.NewReader(payload), KindImage, Hint{Filename: "foto.png"})
	if err != nil {
		t.Fatalf("Upload gagal: %v", err)
	}
	if res.Meta.Mimetype != "image/png" || res.Meta.Size != int64(len(payload)) {
		t.Fatalf("metadata hasil upload tidak sesuai: %+v", res.Meta)
	}
	entries, err := os.ReadDir(tmp)
	if err != nil {
		t.Fatalf("baca tmp setelah Upload gagal: %v", err)
	}
	if len(entries) != 0 {
		t.Fatalf("direktori tmp harus kosong setelah Upload selesai, sisa %d entri", len(entries))
	}
}

// TestUpload_BatasUkuranDiTengahStream memastikan stream yang melebihi batas berhenti di max+1 dengan ErrMediaTooLarge dan tmp tetap kosong.
func TestUpload_BatasUkuranDiTengahStream(t *testing.T) {
	tmp := t.TempDir()
	const maxBytes = 1024
	pipe := NewPipeline(Limits{
		MediaMaxBytes:    maxBytes,
		DocumentMaxBytes: maxBytes,
		TmpDir:           tmp,
	})

	pngHead := makePNGBytes(32, 32)
	srcData := make([]byte, maxBytes+10)
	copy(srcData, pngHead)
	src := &countingReader{data: srcData}

	up := fakeUploader{
		fn: func(ctx context.Context, plaintext io.Reader, tempFile io.ReadWriteSeeker, _ whatsmeow.MediaType) (whatsmeow.UploadResponse, error) {
			if _, err := io.Copy(tempFile, plaintext); err != nil {
				return whatsmeow.UploadResponse{}, err
			}
			return whatsmeow.UploadResponse{}, nil
		},
	}

	_, err := pipe.Upload(context.Background(), up, src, KindImage, Hint{})
	if !errors.Is(err, ErrMediaTooLarge) {
		t.Fatalf("diharapkan ErrMediaTooLarge, didapat: %v", err)
	}
	if src.read.Load() > maxBytes+1 {
		t.Fatalf("byte terbaca dari sumber (%d) melebihi max+1 (%d)", src.read.Load(), maxBytes+1)
	}
	entries, err := os.ReadDir(tmp)
	if err != nil {
		t.Fatalf("baca tmp gagal: %v", err)
	}
	if len(entries) != 0 {
		t.Fatalf("direktori tmp harus kosong setelah gagal ukuran, sisa %d entri", len(entries))
	}
}

// TestUpload_KlienPutusDiTengah memastikan stream yang terputus membersihkan temp dan melepas semaphore media.
func TestUpload_KlienPutusDiTengah(t *testing.T) {
	tmp := t.TempDir()
	pipe := NewPipeline(Limits{
		MediaMaxBytes:    1 << 20,
		DocumentMaxBytes: 1 << 20,
		TmpDir:           tmp,
	})

	sentinel := errors.New("koneksi klien terputus")
	mid := &failingMidReader{
		prefix: makePNGBytes(32, 32),
		failAt: 512,
		err:    sentinel,
	}

	up := fakeUploader{
		fn: func(ctx context.Context, plaintext io.Reader, tempFile io.ReadWriteSeeker, _ whatsmeow.MediaType) (whatsmeow.UploadResponse, error) {
			if _, err := io.Copy(tempFile, plaintext); err != nil {
				return whatsmeow.UploadResponse{}, err
			}
			return whatsmeow.UploadResponse{}, nil
		},
	}

	for i := 0; i < 3; i++ {
		mid.pos = 0
		_, err := pipe.Upload(context.Background(), up, mid, KindImage, Hint{})
		if !errors.Is(err, sentinel) {
			t.Fatalf("iterasi %d: diharapkan error putus stream (%v), didapat: %v", i, sentinel, err)
		}
	}

	entries, err := os.ReadDir(tmp)
	if err != nil {
		t.Fatalf("baca tmp gagal: %v", err)
	}
	if len(entries) != 0 {
		t.Fatalf("direktori tmp harus kosong setelah koneksi putus, sisa %d entri", len(entries))
	}

	// Pastikan semaphore (kapasitas 2) masih terlepas penuh setelah 3 kegagalan berturut-turut.
	okUp := fakeUploader{
		fn: func(ctx context.Context, plaintext io.Reader, tempFile io.ReadWriteSeeker, _ whatsmeow.MediaType) (whatsmeow.UploadResponse, error) {
			n, _ := io.Copy(tempFile, plaintext)
			return whatsmeow.UploadResponse{FileLength: uint64(n)}, nil
		},
	}
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	if _, err := pipe.Upload(ctx, okUp, bytes.NewReader(makePNGBytes(16, 16)), KindImage, Hint{}); err != nil {
		t.Fatalf("semaphore tidak terlepas setelah stream putus: %v", err)
	}
}

// TestSniff_Tabel memverifikasi deteksi mimetype dan validasi per jenis media pada berbagai header.
func TestSniff_Tabel(t *testing.T) {
	mp4Head := []byte{0x00, 0x00, 0x00, 0x18, 'f', 't', 'y', 'p', 'i', 's', 'o', 'm', 0, 0, 2, 0}
	mp3Head := []byte{'I', 'D', '3', 0x03, 0x00, 0, 0, 0, 0, 0}
	pdfHead := []byte("%PDF-1.7\n")

	kasus := []struct {
		nama     string
		head     []byte
		kind     Kind
		hint     Hint
		wantMIME string
		wantErr  error
	}{
		{nama: "jpeg", head: makeJPEGBytes(16, 16), kind: KindImage, wantMIME: "image/jpeg"},
		{nama: "png", head: makePNGBytes(16, 16), kind: KindImage, wantMIME: "image/png"},
		{nama: "webp", head: makeWebPVPXBytes(64, 64), kind: KindImage, wantMIME: "image/webp"},
		{nama: "ogg-opus-ptt", head: makeOggOpusHeader(), kind: KindPTT, wantMIME: "audio/ogg"},
		{nama: "mp3", head: mp3Head, kind: KindAudio, wantMIME: "audio/mpeg"},
		{nama: "pdf", head: pdfHead, kind: KindDocument, hint: Hint{Filename: "berkas.pdf"}, wantMIME: "application/pdf"},
		{nama: "mp4", head: mp4Head, kind: KindVideo, wantMIME: "video/mp4"},
		{nama: "gif-ditolak", head: []byte("GIF89a\x01\x00\x01\x00\x80\x00\x00"), kind: KindImage, wantErr: ErrUnsupportedMedia},
	}

	for _, tc := range kasus {
		t.Run(tc.nama, func(t *testing.T) {
			meta, err := Sniff(tc.head, tc.kind, tc.hint)
			if tc.wantErr != nil {
				if !errors.Is(err, tc.wantErr) {
					t.Fatalf("diharapkan error %v, didapat: %v", tc.wantErr, err)
				}
				return
			}
			if err != nil {
				t.Fatalf("Sniff gagal: %v", err)
			}
			if meta.Mimetype != tc.wantMIME {
				t.Fatalf("mimetype diharapkan %q, didapat %q", tc.wantMIME, meta.Mimetype)
			}
		})
	}
}

// TestSniff_PTTWajibOggOpus memastikan jenis ptt menolak format non-OggOpus dengan ErrPTTRequiresOggOpus.
func TestSniff_PTTWajibOggOpus(t *testing.T) {
	mp3Head := []byte{'I', 'D', '3', 0x03, 0x00, 0, 0, 0, 0, 0}
	if _, err := Sniff(mp3Head, KindPTT, Hint{}); !errors.Is(err, ErrPTTRequiresOggOpus) {
		t.Fatalf("mp3 pada ptt harus menghasilkan ErrPTTRequiresOggOpus, didapat: %v", err)
	}
	oggVorbis := make([]byte, 40)
	copy(oggVorbis[0:4], "OggS")
	copy(oggVorbis[28:35], "\x01vorbis")
	if _, err := Sniff(oggVorbis, KindPTT, Hint{}); !errors.Is(err, ErrPTTRequiresOggOpus) {
		t.Fatalf("ogg non-opus pada ptt harus menghasilkan ErrPTTRequiresOggOpus, didapat: %v", err)
	}
}

// TestSniff_StickerWajibWebP memastikan jenis sticker menolak format selain image/webp dengan ErrStickerRequiresWebP.
func TestSniff_StickerWajibWebP(t *testing.T) {
	if _, err := Sniff(makePNGBytes(64, 64), KindSticker, Hint{}); !errors.Is(err, ErrStickerRequiresWebP) {
		t.Fatalf("png pada sticker harus menghasilkan ErrStickerRequiresWebP, didapat: %v", err)
	}
	meta, err := Sniff(makeWebPVPXBytes(512, 512), KindSticker, Hint{})
	if err != nil || meta.Mimetype != "image/webp" {
		t.Fatalf("webp pada sticker gagal: meta=%+v err=%v", meta, err)
	}
}

// TestSniff_DimensiDariHeader memverifikasi ekstraksi lebar dan tinggi PNG 320x240, JPEG 640x480, serta WebP VP8X.
func TestSniff_DimensiDariHeader(t *testing.T) {
	pngMeta, err := Sniff(makePNGBytes(320, 240), KindImage, Hint{})
	if err != nil || pngMeta.Width != 320 || pngMeta.Height != 240 {
		t.Fatalf("dimensi PNG tidak sesuai: %+v err=%v", pngMeta, err)
	}

	jpgMeta, err := Sniff(makeJPEGBytes(640, 480), KindImage, Hint{})
	if err != nil || jpgMeta.Width != 640 || jpgMeta.Height != 480 {
		t.Fatalf("dimensi JPEG tidak sesuai: %+v err=%v", jpgMeta, err)
	}

	webpMeta, err := Sniff(makeWebPVPXBytes(400, 300), KindSticker, Hint{})
	if err != nil || webpMeta.Width != 400 || webpMeta.Height != 300 {
		t.Fatalf("dimensi WebP VP8X tidak sesuai: %+v err=%v", webpMeta, err)
	}
}

// TestDropBehind_PanggilanSyscall memastikan penulisan 12 MiB dan pembacaannya memicu SyncRange dan DontNeed per chunk 4 MiB secara tepat.
func TestDropBehind_PanggilanSyscall(t *testing.T) {
	const chunk = 4 << 20
	tmp := t.TempDir()
	f, err := os.CreateTemp(tmp, "db-*")
	if err != nil {
		t.Fatalf("buat file test gagal: %v", err)
	}
	defer func() {
		name := f.Name()
		_ = f.Close()
		_ = os.Remove(name)
	}()

	fakeSys := &fakeSyscalls{}
	db := newDropBehindWithSys(f, chunk, fakeSys, true)

	blk := make([]byte, 64<<10)
	totalWrite := 12 << 20
	for off := 0; off < totalWrite; off += len(blk) {
		if _, err := db.Write(blk); err != nil {
			t.Fatalf("Write gagal pada offset %d: %v", off, err)
		}
	}

	expectedWrite := []syscallCall{
		{op: "sync", off: 0, n: chunk},
		{op: "dontneed", off: 0, n: chunk},
		{op: "sync", off: chunk, n: chunk},
		{op: "dontneed", off: chunk, n: chunk},
	}
	if len(fakeSys.calls) != len(expectedWrite) {
		t.Fatalf("jumlah syscall fase tulis tidak sesuai: didapat %+v, diharapkan %+v", fakeSys.calls, expectedWrite)
	}
	for i := range expectedWrite {
		if fakeSys.calls[i] != expectedWrite[i] {
			t.Fatalf("syscall tulis ke-%d: didapat %+v, diharapkan %+v", i, fakeSys.calls[i], expectedWrite[i])
		}
	}

	fakeSys.calls = nil
	if _, err := db.Seek(0, io.SeekStart); err != nil {
		t.Fatalf("Seek gagal: %v", err)
	}
	buf := make([]byte, 64<<10)
	for {
		_, err := db.Read(buf)
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			t.Fatalf("Read gagal: %v", err)
		}
	}

	expectedRead := []syscallCall{
		{op: "dontneed", off: 0, n: chunk},
		{op: "dontneed", off: chunk, n: chunk},
		{op: "dontneed", off: 2 * chunk, n: chunk},
	}
	if len(fakeSys.calls) != len(expectedRead) {
		t.Fatalf("jumlah syscall fase baca tidak sesuai: didapat %+v, diharapkan %+v", fakeSys.calls, expectedRead)
	}
	for i := range expectedRead {
		if fakeSys.calls[i] != expectedRead[i] {
			t.Fatalf("syscall baca ke-%d: didapat %+v, diharapkan %+v", i, fakeSys.calls[i], expectedRead[i])
		}
	}
}

// TestCreateAnonymous_TanpaNama memastikan file temp tidak meninggalkan nama direktori selama terbuka (Linux) dan setelah ditutup (semua OS).
func TestCreateAnonymous_TanpaNama(t *testing.T) {
	dir := t.TempDir()
	f, err := CreateAnonymous(dir)
	if err != nil {
		t.Fatalf("CreateAnonymous gagal: %v", err)
	}
	if _, err := f.Write([]byte("rahasia")); err != nil {
		_ = f.Close()
		t.Fatalf("tulis file anonim gagal: %v", err)
	}

	if runtime.GOOS == "linux" {
		entries, err := os.ReadDir(dir)
		if err != nil {
			_ = f.Close()
			t.Fatalf("baca dir gagal: %v", err)
		}
		if len(entries) != 0 {
			_ = f.Close()
			t.Fatalf("pada Linux direktori harus kosong selagi file terbuka, didapat %d entri", len(entries))
		}
	}

	if err := f.Close(); err != nil {
		t.Fatalf("tutup file anonim gagal: %v", err)
	}
	entries, err := os.ReadDir(dir)
	if err != nil {
		t.Fatalf("baca dir setelah close gagal: %v", err)
	}
	if len(entries) != 0 {
		t.Fatalf("direktori harus kosong setelah file anonim ditutup, sisa %d entri", len(entries))
	}
}

// TestUploads_TTLDanPrune memverifikasi penyimpanan descriptor upload, status kedaluwarsa, dan pembersihan baris kedaluwarsa.
func TestUploads_TTLDanPrune(t *testing.T) {
	ctx := context.Background()
	j, err := journal.Open(ctx, t.TempDir()+"/journal.db", store.DBOptions{MaxConns: 1})
	if err != nil {
		t.Fatalf("buka journal gagal: %v", err)
	}
	defer j.Close()

	us := NewUploadStore(j.DB())
	now := time.Unix(1700000000, 0)

	aktif, err := us.Put(ctx, UploadRecord{
		InstanceID: "wa-1",
		Kind:       KindImage,
		Mimetype:   "image/jpeg",
		Size:       2048,
		Resp: whatsmeow.UploadResponse{
			URL:        "https://mmg.whatsapp.net/ok",
			DirectPath: "/v/ok",
			FileLength: 2048,
		},
		Width:     640,
		Height:    480,
		CreatedAt: now.UnixMilli(),
		ExpiresAt: now.Add(24 * time.Hour).UnixMilli(),
	})
	if err != nil {
		t.Fatalf("Put upload aktif gagal: %v", err)
	}
	if !strings.HasPrefix(aktif.ID, "upl_") {
		t.Fatalf("prefix upload_id harus upl_, didapat %q", aktif.ID)
	}

	lama, err := us.Put(ctx, UploadRecord{
		InstanceID: "wa-1",
		Kind:       KindDocument,
		Mimetype:   "application/pdf",
		Size:       512,
		Resp: whatsmeow.UploadResponse{
			URL:        "https://mmg.whatsapp.net/old",
			DirectPath: "/v/old",
			FileLength: 512,
		},
		Filename:  "dok.pdf",
		CreatedAt: now.Add(-25 * time.Hour).UnixMilli(),
		ExpiresAt: now.Add(-time.Hour).UnixMilli(),
	})
	if err != nil {
		t.Fatalf("Put upload lama gagal: %v", err)
	}

	got, err := us.Get(ctx, "wa-1", aktif.ID, now)
	if err != nil {
		t.Fatalf("Get upload aktif gagal: %v", err)
	}
	if got.Width != 640 || got.Height != 480 || got.Resp.DirectPath != "/v/ok" {
		t.Fatalf("descriptor upload aktif tidak cocok: %+v", got)
	}

	if _, err := us.Get(ctx, "wa-1", lama.ID, now); !errors.Is(err, ErrUploadExpired) {
		t.Fatalf("upload kedaluwarsa harus mengembalikan ErrUploadExpired, didapat: %v", err)
	}
	if _, err := us.Get(ctx, "wa-1", "upl_tidakada", now); !errors.Is(err, ErrUploadNotFound) {
		t.Fatalf("upload hilang harus mengembalikan ErrUploadNotFound, didapat: %v", err)
	}

	pruned, err := us.Prune(ctx, now)
	if err != nil {
		t.Fatalf("Prune uploads gagal: %v", err)
	}
	if pruned != 1 {
		t.Fatalf("jumlah baris yang dipangkas diharapkan 1, didapat %d", pruned)
	}

	if _, err := us.Get(ctx, "wa-1", lama.ID, now); !errors.Is(err, ErrUploadNotFound) {
		t.Fatalf("upload lama seharusnya sudah terhapus, didapat: %v", err)
	}
	if _, err := us.Get(ctx, "wa-1", aktif.ID, now); err != nil {
		t.Fatalf("upload aktif tidak boleh terhapus oleh Prune: %v", err)
	}
}

// BenchmarkUploadPipeline mengukur kinerja satu-pass streaming upload 1 MiB beserta alokasi memori.
func BenchmarkUploadPipeline(b *testing.B) {
	pipe := NewPipeline(Limits{
		MediaMaxBytes:    8 << 20,
		DocumentMaxBytes: 8 << 20,
		TmpDir:           b.TempDir(),
	})
	payload := make([]byte, 1<<20)
	copy(payload, makePNGBytes(64, 64))
	up := fakeUploader{
		fn: func(ctx context.Context, plaintext io.Reader, tempFile io.ReadWriteSeeker, _ whatsmeow.MediaType) (whatsmeow.UploadResponse, error) {
			n, err := io.Copy(tempFile, plaintext)
			return whatsmeow.UploadResponse{FileLength: uint64(n)}, err
		},
	}

	b.ReportAllocs()
	b.SetBytes(int64(len(payload)))
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, err := pipe.Upload(context.Background(), up, bytes.NewReader(payload), KindImage, Hint{}); err != nil {
			b.Fatalf("Benchmark upload gagal: %v", err)
		}
	}
}
