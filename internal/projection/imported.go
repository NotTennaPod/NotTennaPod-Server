package projection

// ImportedRow is one source row of an imported episode (PROTOCOL §11.5):
// its own state and occurrence. When an import groups several rows into one
// episode, its effect carries them (Imported.Rows), so placement sends each
// row where its own occurrence routes and aggregates again the rows that
// land in one scope: a GUID disputed later splits the rows into their
// releases.
type ImportedRow struct {
	SourceItem  int64  `json:"source_item"`
	SourceMedia int64  `json:"source_media"`   // 0 without a media row
	Unit        string `json:"unit,omitempty"` // its statistical unit's ID (import/media row), with a media row
	Ref         *Ref   `json:"ref"`            // the episode the import derived, with the row's own occurrence
	PlayState   string `json:"play_state"`
	Position    int64  `json:"position"`
	Favorite    bool   `json:"favorite,omitempty"`
	LastPlayed  int64  `json:"last_played,omitempty"`
	History     *int64 `json:"history,omitempty"`
	PlayedTime  int64  `json:"played_time,omitempty"`
	Duration    int64  `json:"duration,omitempty"`
	QueueIndex  int    `json:"queue_index"` // its place in the backup's queue, -1 when not queued
	QueueKey    string `json:"queue_key,omitempty"`
	PositionID  string `json:"position_id,omitempty"`
}
