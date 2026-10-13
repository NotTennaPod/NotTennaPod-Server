package media

import (
	"mime"
	"path"
	"regexp"
	"strings"
)

// Capabilities is the fixed list of MIME strings the player asks
// canPlayType about and sends with its answers (PROTOCOL §13). A source is
// described by one of them; the output profiles by others.
var Capabilities = []string{
	`audio/mpeg`,
	`audio/aac`,
	`audio/mp4`,
	`audio/mp4; codecs="mp4a.40.2"`,
	`audio/mp4; codecs="mp4a.40.5"`,
	`audio/mp4; codecs="mp4a.40.29"`,
	`audio/mp4; codecs="alac"`,
	`audio/mp4; codecs="flac"`,
	`audio/mp4; codecs="opus"`,
	`audio/flac`,
	`audio/ogg`,
	`audio/ogg; codecs="vorbis"`,
	`audio/ogg; codecs="opus"`,
	`audio/ogg; codecs="flac"`,
	`audio/webm`,
	`audio/webm; codecs="opus"`,
	`audio/webm; codecs="vorbis"`,
	`audio/wav`,
	`audio/wav; codecs="1"`,
	`audio/aiff`,
	`video/mp4`,
	`video/mp4; codecs="avc1.42E01E, mp4a.40.2"`,
	`video/mp4; codecs="avc1.4D401F, mp4a.40.2"`,
	`video/mp4; codecs="avc1.640028, mp4a.40.2"`,
	`video/webm`,
	`video/webm; codecs="vp8, vorbis"`,
	`video/webm; codecs="vp9, opus"`,
}

// Facts are what a probe learned about a representation (PROTOCOL §13):
// independent of the browser, cached by representation.
type Facts struct {
	Demuxer    string // the -f name the conversion names: mp3, aac, mov, ogg, matroska, flac, wav, aiff
	Audio      string // ffprobe's codec_name of the first audio stream
	Profile    string // its profile (AAC: LC, HE-AAC, HE-AACv2)
	Video      string // the first video stream's codec_name; "" without (cover pictures are not video)
	VideoProf  string
	SampleRate int
	Channels   int
	DurationMS int64 // 0 when unknown
	Estimated  bool  // the duration is ffprobe's estimate from the bitrate
}

// demuxers maps ffprobe's format_name to the demuxer conversions name.
func demuxerOf(formatName string) string {
	for _, part := range strings.Split(formatName, ",") {
		switch part {
		case "mp3", "aac", "ogg", "flac", "wav", "aiff":
			return part
		case "mov", "mp4":
			return "mov"
		case "matroska", "webm":
			return "matroska"
		}
	}
	return ""
}

// formatWhitelist is the demuxers ffprobe and ffmpeg may open (PROTOCOL §13):
// playlists, concat, HLS and the like are refused.
const formatWhitelist = "mp3,ogg,matroska,webm,mov,mp4,m4a,flac,wav,aiff,aac"

// sourceMIME is the capability string describing a probed source, or "" for
// a source no browser plays as it is.
func (f *Facts) sourceMIME() string {
	aac := map[string]string{"LC": "mp4a.40.2", "HE-AAC": "mp4a.40.5", "HE-AACv2": "mp4a.40.29"}
	switch f.Demuxer {
	case "mp3":
		if f.Audio == "mp3" && f.Video == "" {
			return `audio/mpeg`
		}
	case "aac":
		if f.Audio == "aac" && f.Video == "" {
			return `audio/aac`
		}
	case "flac":
		if f.Audio == "flac" && f.Video == "" {
			return `audio/flac`
		}
	case "aiff":
		if strings.HasPrefix(f.Audio, "pcm_") && f.Video == "" {
			return `audio/aiff`
		}
	case "wav":
		if f.Video == "" && (f.Audio == "pcm_s16le" || f.Audio == "pcm_u8" || f.Audio == "pcm_s24le" || f.Audio == "pcm_s32le") {
			return `audio/wav; codecs="1"`
		}
	case "ogg":
		if f.Video == "" && (f.Audio == "vorbis" || f.Audio == "opus" || f.Audio == "flac") {
			return `audio/ogg; codecs="` + f.Audio + `"`
		}
	case "matroska":
		switch {
		case f.Video == "" && (f.Audio == "opus" || f.Audio == "vorbis"):
			return `audio/webm; codecs="` + f.Audio + `"`
		case f.Video == "vp8" && f.Audio == "vorbis":
			return `video/webm; codecs="vp8, vorbis"`
		case f.Video == "vp9" && f.Audio == "opus":
			return `video/webm; codecs="vp9, opus"`
		}
	case "mov":
		if f.Video == "" {
			switch f.Audio {
			case "aac":
				if c, ok := aac[f.Profile]; ok {
					return `audio/mp4; codecs="` + c + `"`
				}
				return `audio/mp4; codecs="mp4a.40.2"`
			case "alac", "flac", "opus":
				return `audio/mp4; codecs="` + f.Audio + `"`
			}
			return ""
		}
		if f.Video == "h264" && f.Audio == "aac" && (f.Profile == "LC" || f.Profile == "") {
			switch f.VideoProf {
			case "Baseline", "Constrained Baseline":
				return `video/mp4; codecs="avc1.42E01E, mp4a.40.2"`
			case "Main":
				return `video/mp4; codecs="avc1.4D401F, mp4a.40.2"`
			case "High":
				return `video/mp4; codecs="avc1.640028, mp4a.40.2"`
			}
		}
	}
	return ""
}

// bareType is the media type a source of these facts is served as.
func (f *Facts) bareType() string {
	switch f.Demuxer {
	case "mp3":
		return "audio/mpeg"
	case "aac":
		return "audio/aac"
	case "flac":
		return "audio/flac"
	case "aiff":
		return "audio/aiff"
	case "wav":
		return "audio/wav"
	case "ogg":
		return "audio/ogg"
	case "matroska":
		if f.Video != "" {
			return "video/webm"
		}
		return "audio/webm"
	case "mov":
		if f.Video != "" {
			return "video/mp4"
		}
		return "audio/mp4"
	}
	return ""
}

// aliases are the media types publishers declare for the types the
// capability list names.
var aliases = map[string]string{
	"audio/mp3": "audio/mpeg", "audio/x-mp3": "audio/mpeg", "audio/mpeg3": "audio/mpeg", "audio/x-mpeg": "audio/mpeg",
	"audio/mpg": "audio/mpeg", "audio/x-mpeg-3": "audio/mpeg",
	"audio/x-m4a": "audio/mp4", "audio/m4a": "audio/mp4", "audio/x-m4b": "audio/mp4", "audio/m4b": "audio/mp4", "audio/mp4a-latm": "audio/mp4",
	"audio/x-aac": "audio/aac", "audio/aacp": "audio/aac", "audio/x-hx-aac-adts": "audio/aac",
	"audio/x-flac": "audio/flac",
	"audio/x-wav":  "audio/wav", "audio/wave": "audio/wav", "audio/vnd.wave": "audio/wav",
	"audio/x-aiff": "audio/aiff",
	"audio/opus":   "audio/ogg", "audio/vorbis": "audio/ogg", "audio/x-ogg": "audio/ogg", "application/ogg": "audio/ogg",
	"video/x-m4v": "video/mp4", "video/quicktime": "video/mp4",
}

// extensions guess a type from the address when nothing better is known.
var extensions = map[string]string{
	".mp3": "audio/mpeg", ".m4a": "audio/mp4", ".m4b": "audio/mp4", ".mp4": "video/mp4", ".m4v": "video/mp4", ".mov": "video/mp4",
	".aac": "audio/aac", ".ogg": "audio/ogg", ".oga": "audio/ogg", ".opus": "audio/ogg", ".flac": "audio/flac",
	".wav": "audio/wav", ".aif": "audio/aiff", ".aiff": "audio/aiff", ".webm": "audio/webm",
}

// mediaType is a declared or served type reduced to its bare, lower-case
// media type, when it names audio or video; "" otherwise.
var safeMedia = regexp.MustCompile(`^(audio|video)/[a-z0-9][a-z0-9.+-]{0,63}$`)

func mediaType(declared string) string {
	mt, _, err := mime.ParseMediaType(declared)
	if err != nil {
		return ""
	}
	mt = strings.ToLower(mt)
	if a, ok := aliases[mt]; ok {
		mt = a
	}
	if !safeMedia.MatchString(mt) || strings.Contains(mt, "xml") || strings.Contains(mt, "html") || strings.Contains(mt, "script") {
		return ""
	}
	return mt
}

// typeFromURL is the type an address's extension suggests.
func typeFromURL(raw string) string {
	p := raw
	if i := strings.IndexAny(p, "?#"); i >= 0 {
		p = p[:i]
	}
	return extensions[strings.ToLower(path.Ext(p))]
}

// plays reports whether the browser's answers say it plays the type.
func plays(caps map[string]string, mimeString string) bool {
	if mimeString == "" {
		return false
	}
	a := caps[mimeString]
	return a == "probably" || a == "maybe"
}

// Profile is a compatibility stream's output format (PROTOCOL §13; the order
// matters because streams of unknown length play in WebKit only as ADTS AAC
// or MP3).
type Profile struct {
	Name string   // aac, mp3, mp4-aac, webm-opus
	MIME string   // the capability string the browser must play
	Type string   // the Content-Type served
	args []string // encoder and muxer
	demx string   // the demuxer that reads the output back (verification)
}

// Profiles in the order the server offers them.
var Profiles = []Profile{
	{Name: "aac", MIME: `audio/aac`, Type: "audio/aac", demx: "aac",
		args: []string{"-c:a", "aac", "-b:a", "128k", "-f", "adts"}},
	{Name: "mp3", MIME: `audio/mpeg`, Type: "audio/mpeg", demx: "mp3",
		args: []string{"-c:a", "libmp3lame", "-b:a", "128k", "-f", "mp3"}},
	{Name: "mp4-aac", MIME: `audio/mp4; codecs="mp4a.40.2"`, Type: "audio/mp4", demx: "mov",
		args: []string{"-c:a", "aac", "-b:a", "128k", "-f", "mp4", "-movflags", "empty_moov+default_base_moof+frag_keyframe", "-frag_duration", "1000000"}},
	{Name: "webm-opus", MIME: `audio/webm; codecs="opus"`, Type: "audio/webm", demx: "matroska",
		args: []string{"-c:a", "libopus", "-b:a", "96k", "-f", "webm"}},
}

func profileNamed(name string) *Profile {
	for i := range Profiles {
		if Profiles[i].Name == name {
			return &Profiles[i]
		}
	}
	return nil
}

// sourceKey names a probed source's row of the coverage matrix (PROTOCOL §13), "" for a combination
// outside it.
func (f *Facts) sourceKey() string {
	key := f.Demuxer + "/" + f.Audio
	if f.Video != "" {
		key = f.Demuxer + "/video+" + f.Audio
	}
	for _, s := range matrixSources {
		if s.key == key {
			return key
		}
	}
	return ""
}
