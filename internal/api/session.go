package api

import (
	"net/http"

	"github.com/NotTennaPod/NotTennaPod-Server/internal/storage"
)

// GET /v1/session tells a page load which account its session belongs to and
// the web installation it is bound to, if any (PROTOCOL §12: the browser
// registers its installation once per profile and account).
func (s *Server) sessionRoutes() {
	s.route("/v1/session", endpoint{method: http.MethodGet, auth: authWeb, supersededOK: true, handle: (*request).session})
}

func (req *request) session() (int, any, error) {
	if _, err := req.query(); err != nil {
		return 0, nil, err
	}
	var login storage.Login
	// The session is checked again in the read that answers.
	err := req.read(func(r *storage.ReadTx) error {
		var err error
		login, err = r.LoginByAccount(req.principal.accountID())
		return err
	})
	if err != nil {
		return 0, nil, err
	}
	var install any
	if id := req.principal.cred.InstallID; id != "" {
		install = id
	}
	superseded := ""
	if i := req.principal.cred.Installation; i != nil {
		superseded = i.SupersededBy
	}
	return http.StatusOK, map[string]any{"account": loginAccountJSON(login), "install_id": install, "superseded_by": nullable(superseded)}, nil
}
