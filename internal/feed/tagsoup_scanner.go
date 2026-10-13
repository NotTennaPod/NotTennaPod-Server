package feed

// The port of TagSoup's HTMLScanner (platform/external/tagsoup, Android 16):
// the table-driven scanner that reports lexical events to the Parser. It
// reads UTF-16 code units, as the Java Reader does.

// HTMLScanner's states.
const (
	tagsoupSAname = iota + 1
	tagsoupSApos
	tagsoupSAval
	tagsoupSBB
	tagsoupSBBC
	tagsoupSBBCD
	tagsoupSBBCDA
	tagsoupSBBCDAT
	tagsoupSBBCDATA
	tagsoupSCdata
	tagsoupSCdata2
	tagsoupSCdsect
	tagsoupSCdsect1
	tagsoupSCdsect2
	tagsoupSCom
	tagsoupSCom2
	tagsoupSCom3
	tagsoupSCom4
	tagsoupSDecl
	tagsoupSDecl2
	tagsoupSDone
	tagsoupSEmptytag
	tagsoupSEnt
	tagsoupSEq
	tagsoupSEtag
	tagsoupSGi
	tagsoupSNcr
	tagsoupSPcdata
	tagsoupSPi
	tagsoupSPitarget
	tagsoupSQuot
	tagsoupSStagc
	tagsoupSTag
	tagsoupSTagws
	tagsoupSXncr
)

// HTMLScanner's actions.
const (
	tagsoupAAdup = iota + 1
	tagsoupAAdupSave
	tagsoupAAdupStagc
	tagsoupAAname
	tagsoupAAnameAdup
	tagsoupAAnameAdupStagc
	tagsoupAAval
	tagsoupAAvalStagc
	tagsoupACdata
	tagsoupACmnt
	tagsoupADecl
	tagsoupAEmptytag
	tagsoupAEntity
	tagsoupAEntityStart
	tagsoupAEtag
	tagsoupAGi
	tagsoupAGiStagc
	tagsoupALt
	tagsoupALtPcdata
	tagsoupAMinus
	tagsoupAMinus2
	tagsoupAMinus3
	tagsoupAPcdata
	tagsoupAPi
	tagsoupAPitarget
	tagsoupAPitargetPi
	tagsoupASave
	tagsoupASkip
	tagsoupASp
	tagsoupAStagc
	tagsoupAUnget
	tagsoupAUnsavePcdata
)

// tagsoupStateTable is HTMLScanner.statetable: state, input character (0 for
// any other, -1 for the end), action, next state.
var tagsoupStateTable = [...][4]int{
	{tagsoupSAname, '/', tagsoupAAnameAdup, tagsoupSEmptytag},
	{tagsoupSAname, '=', tagsoupAAname, tagsoupSAval},
	{tagsoupSAname, '>', tagsoupAAnameAdupStagc, tagsoupSPcdata},
	{tagsoupSAname, 0, tagsoupASave, tagsoupSAname},
	{tagsoupSAname, -1, tagsoupAAnameAdupStagc, tagsoupSDone},
	{tagsoupSAname, ' ', tagsoupAAname, tagsoupSEq},
	{tagsoupSAname, '\n', tagsoupAAname, tagsoupSEq},
	{tagsoupSAname, '\t', tagsoupAAname, tagsoupSEq},
	{tagsoupSApos, '\'', tagsoupAAval, tagsoupSTagws},
	{tagsoupSApos, 0, tagsoupASave, tagsoupSApos},
	{tagsoupSApos, -1, tagsoupAAvalStagc, tagsoupSDone},
	{tagsoupSApos, ' ', tagsoupASp, tagsoupSApos},
	{tagsoupSApos, '\n', tagsoupASp, tagsoupSApos},
	{tagsoupSApos, '\t', tagsoupASp, tagsoupSApos},
	{tagsoupSAval, '"', tagsoupASkip, tagsoupSQuot},
	{tagsoupSAval, '\'', tagsoupASkip, tagsoupSApos},
	{tagsoupSAval, '>', tagsoupAAvalStagc, tagsoupSPcdata},
	{tagsoupSAval, 0, tagsoupASave, tagsoupSStagc},
	{tagsoupSAval, -1, tagsoupAAvalStagc, tagsoupSDone},
	{tagsoupSAval, ' ', tagsoupASkip, tagsoupSAval},
	{tagsoupSAval, '\n', tagsoupASkip, tagsoupSAval},
	{tagsoupSAval, '\t', tagsoupASkip, tagsoupSAval},
	{tagsoupSBB, 'C', tagsoupASkip, tagsoupSBBC},
	{tagsoupSBB, 0, tagsoupASkip, tagsoupSDecl},
	{tagsoupSBB, -1, tagsoupASkip, tagsoupSDone},
	{tagsoupSBBC, 'D', tagsoupASkip, tagsoupSBBCD},
	{tagsoupSBBC, 0, tagsoupASkip, tagsoupSDecl},
	{tagsoupSBBC, -1, tagsoupASkip, tagsoupSDone},
	{tagsoupSBBCD, 'A', tagsoupASkip, tagsoupSBBCDA},
	{tagsoupSBBCD, 0, tagsoupASkip, tagsoupSDecl},
	{tagsoupSBBCD, -1, tagsoupASkip, tagsoupSDone},
	{tagsoupSBBCDA, 'T', tagsoupASkip, tagsoupSBBCDAT},
	{tagsoupSBBCDA, 0, tagsoupASkip, tagsoupSDecl},
	{tagsoupSBBCDA, -1, tagsoupASkip, tagsoupSDone},
	{tagsoupSBBCDAT, 'A', tagsoupASkip, tagsoupSBBCDATA},
	{tagsoupSBBCDAT, 0, tagsoupASkip, tagsoupSDecl},
	{tagsoupSBBCDAT, -1, tagsoupASkip, tagsoupSDone},
	{tagsoupSBBCDATA, '[', tagsoupASkip, tagsoupSCdsect},
	{tagsoupSBBCDATA, 0, tagsoupASkip, tagsoupSDecl},
	{tagsoupSBBCDATA, -1, tagsoupASkip, tagsoupSDone},
	{tagsoupSCdata, '<', tagsoupASave, tagsoupSCdata2},
	{tagsoupSCdata, 0, tagsoupASave, tagsoupSCdata},
	{tagsoupSCdata, -1, tagsoupAPcdata, tagsoupSDone},
	{tagsoupSCdata2, '/', tagsoupAUnsavePcdata, tagsoupSEtag},
	{tagsoupSCdata2, 0, tagsoupASave, tagsoupSCdata},
	{tagsoupSCdata2, -1, tagsoupAUnsavePcdata, tagsoupSDone},
	{tagsoupSCdsect, ']', tagsoupASave, tagsoupSCdsect1},
	{tagsoupSCdsect, 0, tagsoupASave, tagsoupSCdsect},
	{tagsoupSCdsect, -1, tagsoupASkip, tagsoupSDone},
	{tagsoupSCdsect1, ']', tagsoupASave, tagsoupSCdsect2},
	{tagsoupSCdsect1, 0, tagsoupASave, tagsoupSCdsect},
	{tagsoupSCdsect1, -1, tagsoupASkip, tagsoupSDone},
	{tagsoupSCdsect2, '>', tagsoupACdata, tagsoupSPcdata},
	{tagsoupSCdsect2, 0, tagsoupASave, tagsoupSCdsect},
	{tagsoupSCdsect2, -1, tagsoupASkip, tagsoupSDone},
	{tagsoupSCom, '-', tagsoupASkip, tagsoupSCom2},
	{tagsoupSCom, 0, tagsoupASave, tagsoupSCom2},
	{tagsoupSCom, -1, tagsoupACmnt, tagsoupSDone},
	{tagsoupSCom2, '-', tagsoupASkip, tagsoupSCom3},
	{tagsoupSCom2, 0, tagsoupASave, tagsoupSCom2},
	{tagsoupSCom2, -1, tagsoupACmnt, tagsoupSDone},
	{tagsoupSCom3, '-', tagsoupASkip, tagsoupSCom4},
	{tagsoupSCom3, 0, tagsoupAMinus, tagsoupSCom2},
	{tagsoupSCom3, -1, tagsoupACmnt, tagsoupSDone},
	{tagsoupSCom4, '-', tagsoupAMinus3, tagsoupSCom4},
	{tagsoupSCom4, '>', tagsoupACmnt, tagsoupSPcdata},
	{tagsoupSCom4, 0, tagsoupAMinus2, tagsoupSCom2},
	{tagsoupSCom4, -1, tagsoupACmnt, tagsoupSDone},
	{tagsoupSDecl, '-', tagsoupASkip, tagsoupSCom},
	{tagsoupSDecl, '>', tagsoupASkip, tagsoupSPcdata},
	{tagsoupSDecl, '[', tagsoupASkip, tagsoupSBB},
	{tagsoupSDecl, 0, tagsoupASave, tagsoupSDecl2},
	{tagsoupSDecl, -1, tagsoupASkip, tagsoupSDone},
	{tagsoupSDecl2, '>', tagsoupADecl, tagsoupSPcdata},
	{tagsoupSDecl2, 0, tagsoupASave, tagsoupSDecl2},
	{tagsoupSDecl2, -1, tagsoupASkip, tagsoupSDone},
	{tagsoupSEmptytag, '>', tagsoupAEmptytag, tagsoupSPcdata},
	{tagsoupSEmptytag, 0, tagsoupASave, tagsoupSAname},
	{tagsoupSEmptytag, ' ', tagsoupASkip, tagsoupSTagws},
	{tagsoupSEmptytag, '\n', tagsoupASkip, tagsoupSTagws},
	{tagsoupSEmptytag, '\t', tagsoupASkip, tagsoupSTagws},
	{tagsoupSEnt, 0, tagsoupAEntity, tagsoupSEnt},
	{tagsoupSEnt, -1, tagsoupAEntity, tagsoupSDone},
	{tagsoupSEq, '=', tagsoupASkip, tagsoupSAval},
	{tagsoupSEq, '>', tagsoupAAdupStagc, tagsoupSPcdata},
	{tagsoupSEq, 0, tagsoupAAdupSave, tagsoupSAname},
	{tagsoupSEq, -1, tagsoupAAdupStagc, tagsoupSDone},
	{tagsoupSEq, ' ', tagsoupASkip, tagsoupSEq},
	{tagsoupSEq, '\n', tagsoupASkip, tagsoupSEq},
	{tagsoupSEq, '\t', tagsoupASkip, tagsoupSEq},
	{tagsoupSEtag, '>', tagsoupAEtag, tagsoupSPcdata},
	{tagsoupSEtag, 0, tagsoupASave, tagsoupSEtag},
	{tagsoupSEtag, -1, tagsoupAEtag, tagsoupSDone},
	{tagsoupSEtag, ' ', tagsoupASkip, tagsoupSEtag},
	{tagsoupSEtag, '\n', tagsoupASkip, tagsoupSEtag},
	{tagsoupSEtag, '\t', tagsoupASkip, tagsoupSEtag},
	{tagsoupSGi, '/', tagsoupASkip, tagsoupSEmptytag},
	{tagsoupSGi, '>', tagsoupAGiStagc, tagsoupSPcdata},
	{tagsoupSGi, 0, tagsoupASave, tagsoupSGi},
	{tagsoupSGi, -1, tagsoupASkip, tagsoupSDone},
	{tagsoupSGi, ' ', tagsoupAGi, tagsoupSTagws},
	{tagsoupSGi, '\n', tagsoupAGi, tagsoupSTagws},
	{tagsoupSGi, '\t', tagsoupAGi, tagsoupSTagws},
	{tagsoupSNcr, 0, tagsoupAEntity, tagsoupSNcr},
	{tagsoupSNcr, -1, tagsoupAEntity, tagsoupSDone},
	{tagsoupSPcdata, '&', tagsoupAEntityStart, tagsoupSEnt},
	{tagsoupSPcdata, '<', tagsoupAPcdata, tagsoupSTag},
	{tagsoupSPcdata, 0, tagsoupASave, tagsoupSPcdata},
	{tagsoupSPcdata, -1, tagsoupAPcdata, tagsoupSDone},
	{tagsoupSPi, '>', tagsoupAPi, tagsoupSPcdata},
	{tagsoupSPi, 0, tagsoupASave, tagsoupSPi},
	{tagsoupSPi, -1, tagsoupAPi, tagsoupSDone},
	{tagsoupSPitarget, '>', tagsoupAPitargetPi, tagsoupSPcdata},
	{tagsoupSPitarget, 0, tagsoupASave, tagsoupSPitarget},
	{tagsoupSPitarget, -1, tagsoupAPitargetPi, tagsoupSDone},
	{tagsoupSPitarget, ' ', tagsoupAPitarget, tagsoupSPi},
	{tagsoupSPitarget, '\n', tagsoupAPitarget, tagsoupSPi},
	{tagsoupSPitarget, '\t', tagsoupAPitarget, tagsoupSPi},
	{tagsoupSQuot, '"', tagsoupAAval, tagsoupSTagws},
	{tagsoupSQuot, 0, tagsoupASave, tagsoupSQuot},
	{tagsoupSQuot, -1, tagsoupAAvalStagc, tagsoupSDone},
	{tagsoupSQuot, ' ', tagsoupASp, tagsoupSQuot},
	{tagsoupSQuot, '\n', tagsoupASp, tagsoupSQuot},
	{tagsoupSQuot, '\t', tagsoupASp, tagsoupSQuot},
	{tagsoupSStagc, '>', tagsoupAAvalStagc, tagsoupSPcdata},
	{tagsoupSStagc, 0, tagsoupASave, tagsoupSStagc},
	{tagsoupSStagc, -1, tagsoupAAvalStagc, tagsoupSDone},
	{tagsoupSStagc, ' ', tagsoupAAval, tagsoupSTagws},
	{tagsoupSStagc, '\n', tagsoupAAval, tagsoupSTagws},
	{tagsoupSStagc, '\t', tagsoupAAval, tagsoupSTagws},
	{tagsoupSTag, '!', tagsoupASkip, tagsoupSDecl},
	{tagsoupSTag, '/', tagsoupASkip, tagsoupSEtag},
	{tagsoupSTag, '<', tagsoupASave, tagsoupSTag},
	{tagsoupSTag, '?', tagsoupASkip, tagsoupSPitarget},
	{tagsoupSTag, 0, tagsoupASave, tagsoupSGi},
	{tagsoupSTag, -1, tagsoupALtPcdata, tagsoupSDone},
	{tagsoupSTag, ' ', tagsoupALt, tagsoupSPcdata},
	{tagsoupSTag, '\n', tagsoupALt, tagsoupSPcdata},
	{tagsoupSTag, '\t', tagsoupALt, tagsoupSPcdata},
	{tagsoupSTagws, '/', tagsoupASkip, tagsoupSEmptytag},
	{tagsoupSTagws, '>', tagsoupAStagc, tagsoupSPcdata},
	{tagsoupSTagws, 0, tagsoupASave, tagsoupSAname},
	{tagsoupSTagws, -1, tagsoupAStagc, tagsoupSDone},
	{tagsoupSTagws, ' ', tagsoupASkip, tagsoupSTagws},
	{tagsoupSTagws, '\n', tagsoupASkip, tagsoupSTagws},
	{tagsoupSTagws, '\t', tagsoupASkip, tagsoupSTagws},
	{tagsoupSXncr, 0, tagsoupAEntity, tagsoupSXncr},
	{tagsoupSXncr, -1, tagsoupAEntity, tagsoupSDone},
}

// tagsoupWinMap is HTMLScanner.theWinMap: U+0080-U+009F read as windows-1252.
var tagsoupWinMap = [32]uint16{
	0x20AC, 0xFFFD, 0x201A, 0x0192, 0x201E, 0x2026, 0x2020, 0x2021,
	0x02C6, 0x2030, 0x0160, 0x2039, 0x0152, 0xFFFD, 0x017D, 0xFFFD,
	0xFFFD, 0x2018, 0x2019, 0x201C, 0x201D, 0x2022, 0x2013, 0x2014,
	0x02DC, 0x2122, 0x0161, 0x203A, 0x0153, 0xFFFD, 0x017E, 0x0178}

// tagsoupIndex is HTMLScanner.statetableIndex: the row of tagsoupStateTable
// for a state and a character up to ']' (the highest in the table), shifted
// by 2 so that -2 (any other character) and -1 (the end) are indexes too.
var tagsoupIndex = func() (index [tagsoupSXncr + 1][']' + 3]int16) {
	for state := range index {
		for ch := -2; ch <= ']'; ch++ {
			hit := -1
			for i, row := range tagsoupStateTable {
				if row[0] != state {
					continue
				}
				if row[1] == 0 {
					hit = i
				} else if row[1] == ch {
					hit = i
					break
				}
			}
			index[state][ch+2] = int16(hit)
		}
	}
	return index
}()

// tagsoupScanner is HTMLScanner's state over a UTF-16 source.
type tagsoupScanner struct {
	src       []uint16
	pos       int
	pushback  []int
	state     int
	nextState int
	out       []uint16 // theOutputBuffer[0:theSize]
	outCap    int      // theOutputBuffer.length, which decides when text is flushed
}

func (s *tagsoupScanner) read() int {
	if n := len(s.pushback); n > 0 {
		c := s.pushback[n-1]
		s.pushback = s.pushback[:n-1]
		return c
	}
	if s.pos >= len(s.src) {
		return -1
	}
	s.pos++
	return int(s.src[s.pos-1])
}

func (s *tagsoupScanner) unread(c int) {
	if c != -1 {
		s.pushback = append(s.pushback, c)
	}
}

// startCDATA is HTMLScanner.startCDATA: only an end tag ends the content.
func (s *tagsoupScanner) startCDATA() { s.nextState = tagsoupSCdata }

// save is HTMLScanner.save: in text the full buffer is flushed as
// characters; elsewhere it grows.
func (s *tagsoupScanner) save(ch int, h *tagsoupParser) {
	if len(s.out) >= s.outCap-20 {
		if s.state == tagsoupSPcdata || s.state == tagsoupSCdata {
			h.pcdata(s.out)
			s.out = s.out[:0]
		} else {
			s.outCap *= 2
		}
	}
	s.out = append(s.out, uint16(ch))
}

// scan is HTMLScanner.scan.
func (s *tagsoupScanner) scan(h *tagsoupParser) {
	s.state = tagsoupSPcdata
	s.outCap = 200
	if first := s.read(); first != 0xFEFF { // remove a leading BOM
		s.unread(first)
	}
	for n := 0; s.state != tagsoupSDone; n++ {
		if n%4096 == 0 && h.ctx != nil {
			if err := h.ctx.Err(); err != nil {
				panic(parseCanceled{err})
			}
		}
		ch := s.read()
		if ch >= 0x80 && ch <= 0x9F {
			ch = int(tagsoupWinMap[ch-0x80])
		}
		if ch == '\r' {
			ch = s.read() // expect LF next
			if ch != '\n' {
				s.unread(ch)
				ch = '\n'
			}
		}
		if !(ch >= 0x20 || ch == '\n' || ch == '\t' || ch == -1) {
			continue
		}
		adj := ch
		if adj > ']' {
			adj = -2
		}
		action := 0
		if row := tagsoupIndex[s.state][adj+2]; row >= 0 {
			action = tagsoupStateTable[row][2]
			s.nextState = tagsoupStateTable[row][3]
		}
		size := len(s.out)
		switch action {
		case tagsoupAAdup:
			h.adup()
			s.out = s.out[:0]
		case tagsoupAAdupSave:
			h.adup()
			s.out = s.out[:0]
			s.save(ch, h)
		case tagsoupAAdupStagc:
			h.adup()
			s.out = s.out[:0]
			h.stagc(s.out)
		case tagsoupAAname:
			h.aname(s.out)
			s.out = s.out[:0]
		case tagsoupAAnameAdup:
			h.aname(s.out)
			s.out = s.out[:0]
			h.adup()
		case tagsoupAAnameAdupStagc:
			h.aname(s.out)
			s.out = s.out[:0]
			h.adup()
			h.stagc(s.out)
		case tagsoupAAval:
			h.aval(s.out)
			s.out = s.out[:0]
		case tagsoupAAvalStagc:
			h.aval(s.out)
			s.out = s.out[:0]
			h.stagc(s.out)
		case tagsoupACdata:
			if size > 1 {
				s.out = s.out[:size-2] // the "]]"
			}
			h.pcdata(s.out)
			s.out = s.out[:0]
		case tagsoupAEntityStart:
			h.pcdata(s.out)
			s.out = s.out[:0]
			s.save(ch, h)
		case tagsoupAEntity:
			s.entity(ch, h)
		case tagsoupAEtag:
			h.etag(s.out)
			s.out = s.out[:0]
		case tagsoupADecl:
			h.decl(s.out)
			s.out = s.out[:0]
		case tagsoupAGi:
			h.gi(s.out)
			s.out = s.out[:0]
		case tagsoupAGiStagc:
			h.gi(s.out)
			s.out = s.out[:0]
			h.stagc(s.out)
		case tagsoupALt:
			s.save('<', h)
			s.save(ch, h)
		case tagsoupALtPcdata:
			s.save('<', h)
			h.pcdata(s.out)
			s.out = s.out[:0]
		case tagsoupAPcdata:
			h.pcdata(s.out)
			s.out = s.out[:0]
		case tagsoupACmnt, tagsoupAPi:
			// Comments and processing instructions add no text.
			s.out = s.out[:0]
		case tagsoupAMinus3:
			s.save('-', h)
			s.save(' ', h)
		case tagsoupAMinus2:
			s.save('-', h)
			s.save(' ', h)
			s.save('-', h)
			s.save(ch, h)
		case tagsoupAMinus:
			s.save('-', h)
			s.save(ch, h)
		case tagsoupAPitarget:
			s.out = s.out[:0]
		case tagsoupAPitargetPi:
			s.out = s.out[:0]
		case tagsoupASave:
			s.save(ch, h)
		case tagsoupASkip:
		case tagsoupASp:
			s.save(' ', h)
		case tagsoupAStagc:
			h.stagc(s.out)
			s.out = s.out[:0]
		case tagsoupAEmptytag:
			if size > 0 {
				h.gi(s.out)
			}
			s.out = s.out[:0]
			h.stage(s.out)
		case tagsoupAUnget:
			s.unread(ch)
		case tagsoupAUnsavePcdata:
			if size > 0 {
				s.out = s.out[:size-1]
			}
			h.pcdata(s.out)
			s.out = s.out[:0]
		default:
			panic("tagsoup: no action") // HTMLScanner throws an Error; the table has a row for every case
		}
		s.state = s.nextState
	}
	h.eof()
}

// entity is the A_ENTITY action: it extends a character reference by ch, or
// ends it and replaces it by its character.
func (s *tagsoupScanner) entity(ch int, h *tagsoupParser) {
	ch1 := uint16(ch) // (char) ch: the end of the input is U+FFFF
	switch {
	case s.state == tagsoupSEnt && ch1 == '#':
		s.nextState = tagsoupSNcr
		s.save(ch, h)
		return
	case s.state == tagsoupSNcr && (ch1 == 'x' || ch1 == 'X'):
		s.nextState = tagsoupSXncr
		s.save(ch, h)
		return
	case s.state == tagsoupSEnt && (tagsoupIsLetter(ch1) || tagsoupIsDigit(ch1)),
		s.state == tagsoupSNcr && tagsoupIsDigit(ch1),
		s.state == tagsoupSXncr && (tagsoupIsDigit(ch1) || (ch1 >= 'a' && ch1 <= 'f') || (ch1 >= 'A' && ch1 <= 'F')):
		s.save(ch, h)
		return
	}
	ent := h.lookupEntity(s.out[1:])
	if ent != 0 {
		s.out = s.out[:0]
		if ent >= 0x80 && ent <= 0x9F {
			ent = int32(tagsoupWinMap[ent-0x80])
		}
		switch {
		case ent < 0x20:
			// HTMLScanner sets ent to a space but saves nothing.
		case ent >= 0xD800 && ent <= 0xDFFF:
		case ent <= 0xFFFF:
			s.save(int(ent), h)
		default:
			ent -= 0x10000
			s.save(int(ent>>10)+0xD800, h)
			s.save(int(ent&0x3FF)+0xDC00, h)
		}
		if ch != ';' {
			s.unread(ch)
		}
	} else {
		s.unread(ch)
	}
	s.nextState = tagsoupSPcdata
}
