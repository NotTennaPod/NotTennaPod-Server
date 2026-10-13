package ops

import (
	"github.com/NotTennaPod/NotTennaPod-Server/internal/projection"
	"github.com/NotTennaPod/NotTennaPod-Server/internal/protocol"
)

// CredentialURL is, for a podcast's live (else latest) generation, the
// refresh URL the user had set when its current secret was set: its opening
// URL or a change_url, never a server move. A secret stored without approved
// origins approves only that URL's origin, so neither an automatic move nor a
// later URL change extends where its credentials go (PROTOCOL §9.4).
func CredentialURL(ctx *protocol.ApplyContext, podcastID string) (string, error) {
	rows, err := ctx.Account.Contributions(protocol.PodcastScope(podcastID))
	if err != nil {
		return "", err
	}
	var cs []projection.Contribution
	for _, r := range rows {
		e, err := decodeEffect(r.Effect)
		if err != nil {
			return "", err
		}
		if !projection.IsCatalogKind(e.Kind) {
			cs = append(cs, projection.Contribution{Node: r.NodeID, Rank: r.Rank, Effect: e})
		}
	}
	st := projection.FoldPodcast(cs)
	g := st.Live
	if g == nil {
		g = st.Latest
	}
	if g == nil {
		return "", nil
	}
	return g.SecretURL, nil
}
