package api

import "net/http"

// The service page (PROTOCOL §14): how the server is doing,
// for an administrator's web session only. What it shows is assembled by the
// server's main (version, data directory, backups, keys, refresh, listen
// address and warnings), with no credentials, URL userinfo or tokens.
func (s *Server) serviceRoutes() {
	if s.cfg.Service == nil {
		return
	}
	s.route("/v1/admin/service", endpoint{method: http.MethodGet, auth: authAdmin, handle: (*request).service})
}

func (req *request) service() (int, any, error) {
	if _, err := req.query(); err != nil {
		return 0, nil, err
	}
	v, err := req.s.cfg.Service(req.ctx)
	if err != nil {
		return 0, nil, err
	}
	// The administrator is checked again, in a read of its own, before the
	// report goes out.
	if err := req.checkCaller(); err != nil {
		return 0, nil, err
	}
	return http.StatusOK, v, nil
}
