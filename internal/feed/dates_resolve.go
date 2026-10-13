package feed

// From a pattern's fields to an instant: the date is year, month and day of
// month on the Julian calendar before 15 October 1582 and the Gregorian one
// from then; the zone offset is the one read, or the named zone's at that
// local time. A strict pattern also needs every field in range and every
// field to come back unchanged from the instant (so a wrong weekday, 31 April
// or a time in a daylight-saving gap fails). All instant arithmetic wraps.

// dateField indexes dateFields.
type dateField int

const (
	dateYear    dateField = iota
	dateMonth             // 0 = January
	dateDay               // of the month
	dateWeekday           // 1 = Sunday
	dateHour              // of the day
	dateMinute
	dateSecond
	dateMillis
	dateZoneOffset // ms
	dateDSTOffset  // ms
	dateFieldCount
)

// dateStrictRanges are the ranges a strict pattern accepts.
var dateStrictRanges = [dateFieldCount][2]int64{
	dateYear:       {1, 292_278_994},
	dateMonth:      {0, 11},
	dateDay:        {1, 31},
	dateWeekday:    {1, 7},
	dateHour:       {0, 23},
	dateMinute:     {0, 59},
	dateSecond:     {0, 59},
	dateMillis:     {0, 999},
	dateZoneOffset: {-13 * 3_600_000, 14 * 3_600_000},
	dateDSTOffset:  {0, 2 * 3_600_000},
}

// dateFields are what a pattern has read.
type dateFields struct {
	val           [dateFieldCount]int64
	set           [dateFieldCount]bool
	zone          *dateZone // the zone of a zone name; nil is GMT
	ambiguousYear bool      // a two-digit year equal to the window's start
}

func (f *dateFields) record(k dateField, v int64) { f.val[k], f.set[k] = v, true }

func (f *dateFields) forget(k dateField) { f.val[k], f.set[k] = 0, false }

const (
	dateDayMs     = 86_400_000
	dateEpochDay  = 719_163 // 1970-01-01 as a day number (0001-01-01 Gregorian is 1)
	dateCutover   = 577_736 // 1582-10-15, the first Gregorian day
	dateJulianDay = -1      // 0001-01-01 Julian as a day number
)

// resolve turns the fields into an instant.
func (p *dateParser) resolve(f *dateFields, lenient bool) (int64, bool) {
	if !lenient {
		for k := range dateFieldCount {
			if f.set[k] && (f.val[k] < dateStrictRanges[k][0] || f.val[k] > dateStrictRanges[k][1]) {
				return 0, false
			}
		}
	}
	year := int64(1970)
	if f.set[dateYear] {
		year = f.val[dateYear]
	}
	tod := ((f.val[dateHour]*60+f.val[dateMinute])*60+f.val[dateSecond])*1000 + f.val[dateMillis]
	month := f.val[dateMonth]
	year += dateFloorDiv(month, 12)
	month = dateFloorMod(month, 12) + 1
	add := dateFloorDiv(tod, dateDayMs)
	if f.set[dateDay] {
		add += f.val[dateDay] - 1
	}
	greg := dateGregorianDay(year, month, 1) + add
	jul := dateJulianDayOf(year, month, 1) + add
	var day int64
	switch {
	case year < 1582:
		day = jul
	case greg >= dateCutover:
		day = greg
	case jul < dateCutover || lenient:
		// Strictly, the ten days the reform dropped are no date.
		day = jul
	default:
		return 0, false
	}
	local := (day-dateEpochDay)*dateDayMs + dateFloorMod(tod, dateDayMs)

	var instant int64
	z := f.zone
	switch {
	case f.set[dateZoneOffset]:
		// A numeric offset or GMT, always read with a DST offset.
		instant = local - f.val[dateZoneOffset] - f.val[dateDSTOffset]
	case f.set[dateDSTOffset]:
		raw, _ := z.offsetsAt(local - z.rawOffset())
		instant = local - raw - f.val[dateDSTOffset]
	default:
		u := local - z.rawOffset()
		raw, dst := z.offsetsAt(u)
		if dst != 0 && !z.daylightAt(u-dst) {
			dst = 0
		}
		instant = local - raw - dst
	}
	if !lenient && !f.roundTrips(instant) {
		return 0, false
	}
	return instant, true
}

// roundTrips reports whether every field read comes back from the instant.
func (f *dateFields) roundTrips(t int64) bool {
	raw, dst := f.zone.offsetsAt(t)
	if f.set[dateZoneOffset] {
		raw = f.val[dateZoneOffset]
	}
	if f.set[dateDSTOffset] {
		dst = f.val[dateDSTOffset]
	}
	local := t + raw + dst
	day := dateFloorDiv(local, dateDayMs) + dateEpochDay
	ms := dateFloorMod(local, dateDayMs)
	var y, m, d int64
	if day >= dateCutover {
		y, m, d = dateGregorianDate(day)
	} else {
		y, m, d = dateJulianDate(day)
	}
	if y < 1 {
		y = 1 - y // the year of the era BC
	}
	got := [...]int64{
		dateYear:    y,
		dateMonth:   m - 1,
		dateDay:     d,
		dateWeekday: dateFloorMod(day, 7) + 1,
		dateHour:    ms / 3_600_000,
		dateMinute:  ms / 60_000 % 60,
		dateSecond:  ms / 1000 % 60,
		dateMillis:  ms % 1000,
	}
	for k, v := range got {
		if f.set[k] && f.val[k] != v {
			return false
		}
	}
	return true
}

func dateFloorDiv(a, b int64) int64 {
	q := a / b
	if a%b != 0 && (a < 0) != (b < 0) {
		q--
	}
	return q
}

func dateFloorMod(a, b int64) int64 { return a - dateFloorDiv(a, b)*b }

func dateGregorianLeap(y int64) bool { return y%4 == 0 && (y%100 != 0 || y%400 == 0) }

func dateJulianLeap(y int64) bool { return y%4 == 0 }

// dateMonthStart is the day of the year before month m starts (with
// February counted as 30 days), corrected for February.
func dateMonthStart(m int64, leap bool) int64 {
	d := dateFloorDiv(367*m-362, 12)
	if m > 2 {
		if leap {
			d--
		} else {
			d -= 2
		}
	}
	return d
}

// dateGregorianDay is the day number of a proleptic Gregorian date
// (astronomical years: 0 is 1 BC).
func dateGregorianDay(y, m, d int64) int64 {
	y1 := y - 1
	return 365*y1 + dateFloorDiv(y1, 4) - dateFloorDiv(y1, 100) + dateFloorDiv(y1, 400) + dateMonthStart(m, dateGregorianLeap(y)) + d
}

// dateJulianDayOf is the day number of a proleptic Julian date
// (astronomical years).
func dateJulianDayOf(y, m, d int64) int64 {
	y1 := y - 1
	return dateJulianDay - 1 + 365*y1 + dateFloorDiv(y1, 4) + dateMonthStart(m, dateJulianLeap(y)) + d
}

// dateGregorianDate is the proleptic Gregorian date of a day number.
func dateGregorianDate(day int64) (y, m, d int64) {
	d0 := day - 1
	n400, d1 := dateFloorDiv(d0, 146_097), dateFloorMod(d0, 146_097)
	n100, d2 := d1/36_524, d1%36_524
	n4, d3 := d2/1461, d2%1461
	n1 := d3 / 365
	y = 400*n400 + 100*n100 + 4*n4 + n1
	if n100 != 4 && n1 != 4 {
		y++
	}
	m, d = dateMonthOf(day, y, dateGregorianDay)
	return y, m, d
}

// dateJulianDate is the proleptic Julian date of a day number.
func dateJulianDate(day int64) (y, m, d int64) {
	y = dateFloorDiv(4*(day-dateJulianDay)+1464, 1461)
	m, d = dateMonthOf(day, y, dateJulianDayOf)
	return y, m, d
}

// dateMonthOf finds the month and day of a day number in year y.
func dateMonthOf(day, y int64, dayOf func(y, m, d int64) int64) (m, d int64) {
	prior := day - dayOf(y, 1, 1)
	var correction int64
	if day >= dayOf(y, 3, 1) {
		correction = 2
		if dayOf(y, 3, 1)-dayOf(y, 2, 1) == 29 {
			correction = 1
		}
	}
	m = dateFloorDiv(12*(prior+correction)+373, 367)
	return m, day - dayOf(y, m, 1) + 1
}

// dateYearWindow is where two-digit years fall: the 100 years from 80 years
// before now (UTC, proleptic Gregorian).
type dateYearWindow struct {
	start     int64 // the window's first instant
	base      int64 // its start year's century
	ambiguous int64 // its start year within the century (negative for a negative year)
}

func (p *dateParser) yearWindow() *dateYearWindow {
	if p.window == nil {
		day := dateFloorDiv(p.now, dateDayMs)
		y, m, d := dateGregorianDate(day + dateEpochDay)
		y -= 80
		if m == 2 && d == 29 && !dateGregorianLeap(y) {
			d = 28 // the only day that can be missing 80 years earlier
		}
		start := (dateGregorianDay(y, m, d)-dateEpochDay)*dateDayMs + dateFloorMod(p.now, dateDayMs)
		p.window = &dateYearWindow{start: start, base: y / 100 * 100, ambiguous: y % 100}
	}
	return p.window
}
