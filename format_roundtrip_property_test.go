package arj

import (
	"io/fs"
	"testing"
	"time"
)

func TestUnixModeConversionRoundTripProperty(t *testing.T) {
	typeBits := []fs.FileMode{
		0,
		fs.ModeDir,
		fs.ModeSymlink,
		fs.ModeNamedPipe,
		fs.ModeSocket,
		fs.ModeDevice,
		fs.ModeDevice | fs.ModeCharDevice,
	}

	for _, typ := range typeBits {
		for perm := 0; perm <= 0o777; perm++ {
			for specialMask := 0; specialMask < 8; specialMask++ {
				mode := typ | fs.FileMode(perm)
				if specialMask&0b001 != 0 {
					mode |= fs.ModeSetuid
				}
				if specialMask&0b010 != 0 {
					mode |= fs.ModeSetgid
				}
				if specialMask&0b100 != 0 {
					mode |= fs.ModeSticky
				}

				unixMode := fileModeToUnixMode(mode)
				got := unixModeToFileMode(unixMode)
				if got != mode {
					t.Fatalf("unix mode round-trip mismatch: mode=%#o unix=%#o got=%#o", mode, unixMode, got)
				}
				if unixRoundTrip := fileModeToUnixMode(got); unixRoundTrip != unixMode {
					t.Fatalf("unix mode idempotence mismatch: unix=%#o round=%#o", unixMode, unixRoundTrip)
				}
			}
		}
	}
}

func TestDosDateTimeRoundTripProperty(t *testing.T) {
	samples := []time.Time{
		time.Time{},
		time.Date(1979, time.December, 31, 23, 59, 59, 999999999, time.UTC),
		time.Date(1980, time.January, 1, 0, 0, 0, 1, time.UTC),
		time.Date(2107, time.December, 31, 23, 59, 59, 999999999, time.UTC),
		time.Date(2108, time.January, 1, 0, 0, 1, 0, time.UTC),
	}

	locs := []*time.Location{
		time.UTC,
		time.FixedZone("minus7", -7*3600),
		time.FixedZone("plus530", 5*3600+30*60),
	}

	state := uint64(0x9e3779b97f4a7c15)
	next := func() uint64 {
		state = state*6364136223846793005 + 1
		return state
	}

	for i := 0; i < 2048; i++ {
		year := 1900 + int(next()%300) // [1900,2199]
		month := time.Month(1 + next()%12)
		day := 1 + int(next()%28)
		hour := int(next() % 24)
		minute := int(next() % 60)
		second := int(next() % 60)
		nsec := int(next() % 1_000_000_000)
		loc := locs[int(next()%uint64(len(locs)))]

		samples = append(samples, time.Date(year, month, day, hour, minute, second, nsec, loc))
	}

	for _, sample := range samples {
		dos := timeToDosDateTime(sample)
		got := dosDateTimeToTime(dos)
		want := normalizeDOSTime(sample)

		if !got.Equal(want) {
			t.Fatalf("dos time round-trip mismatch: input=%s dos=%#08x got=%s want=%s", sample, dos, got, want)
		}

		if got.IsZero() {
			if dos != 0 {
				t.Fatalf("zero time encoded to non-zero DOS value: %#08x", dos)
			}
			continue
		}

		if dosRoundTrip := timeToDosDateTime(got); dosRoundTrip != dos {
			t.Fatalf("DOS value idempotence mismatch: dos=%#08x round=%#08x", dos, dosRoundTrip)
		}
		if got.Location() != time.UTC {
			t.Fatalf("decoded location = %s, want UTC", got.Location())
		}
		if got.Nanosecond() != 0 {
			t.Fatalf("decoded nanoseconds = %d, want 0", got.Nanosecond())
		}
		if got.Second()%2 != 0 {
			t.Fatalf("decoded second = %d, want even second", got.Second())
		}
		if got.Year() < 1980 || got.Year() > 2107 {
			t.Fatalf("decoded year = %d, want [1980,2107]", got.Year())
		}
	}
}

func normalizeDOSTime(in time.Time) time.Time {
	if in.IsZero() {
		return time.Time{}
	}

	t := in.UTC()
	year := t.Year()
	if year < 1980 {
		t = time.Date(1980, t.Month(), t.Day(), t.Hour(), t.Minute(), t.Second(), 0, time.UTC)
	} else if year > 2107 {
		t = time.Date(2107, t.Month(), t.Day(), t.Hour(), t.Minute(), t.Second(), 0, time.UTC)
	}

	return time.Date(
		t.Year(),
		t.Month(),
		t.Day(),
		t.Hour(),
		t.Minute(),
		(t.Second()/2)*2,
		0,
		time.UTC,
	)
}
