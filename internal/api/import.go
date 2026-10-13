package api

import (
	"encoding/base64"
	"encoding/json"
	"errors"
	"net/http"
	"strconv"

	"github.com/NotTennaPod/NotTennaPod-Server/internal/importer"
	"github.com/NotTennaPod/NotTennaPod-Server/internal/storage"
)

// The library import (PROTOCOL §11.5): the account's own web session, bound to
// its web installation, uploads a file in chunks and follows its import to
// publication.

func (s *Server) importRoutes() {
	s.route("/v1/import",
		endpoint{method: http.MethodGet, auth: authWeb, handle: (*request).currentImport},
		endpoint{method: http.MethodPost, auth: authWeb, handle: (*request).reserveImport})
	s.route("/v1/import/{id}", endpoint{method: http.MethodGet, auth: authWeb, handle: (*request).getImport})
	// A chunk is JSON {data: base64} of at most importer.MaxChunk bytes.
	s.route("/v1/import/{id}/chunks/{n}", endpoint{method: http.MethodPut, auth: authWeb, maxBody: importer.MaxChunk*4/3 + 1024,
		handle: (*request).importChunk})
	s.route("/v1/import/{id}/finish", endpoint{method: http.MethodPost, auth: authWeb, handle: (*request).finishImport})
}

// importer returns the runner, refusing an unbound web session (PROTOCOL §5: the
// web client's own installation imports).
func (req *request) importer() (*importer.Runner, error) {
	if req.s.cfg.Imports == nil {
		return nil, errNotFound
	}
	if req.principal.cred.Installation == nil {
		return nil, fail(http.StatusForbidden, "forbidden", "Register this browser before importing a library.")
	}
	return req.s.cfg.Imports, nil
}

func importError(err error) error {
	switch {
	case errors.Is(err, importer.ErrNotFound):
		return errNotFound
	case errors.Is(err, importer.ErrBadRequest):
		return invalid("The import request is not valid.")
	case errors.Is(err, importer.ErrNotUploading):
		return fail(http.StatusConflict, "import_not_uploading", "This import no longer accepts parts of its file.")
	case errors.Is(err, importer.ErrMismatch):
		return invalid("The uploaded file does not match its size and fingerprint; send its parts again.")
	case errors.Is(err, importer.ErrTooLarge):
		return invalid("The parts sent are larger than the file's size.")
	case errors.Is(err, importer.ErrNoSpace):
		return fail(http.StatusInsufficientStorage, "insufficient_space",
			"The server does not have enough free disk space for this import. Free some space on the server, then try again.")
	case errors.Is(err, storage.ErrImportExists):
		return fail(http.StatusConflict, "import_exists",
			"This account already has a library import. Only one library can be imported into an account; nothing was changed.")
	case errors.Is(err, storage.ErrAccountNotEmpty):
		return fail(http.StatusConflict, "account_not_empty",
			"This account already has a library, so an import would overwrite or mix it. Import into a new, empty account instead.")
	}
	return err
}

// authorize re-validates the session inside the import's transaction.
func (req *request) authorize() importer.Authorize {
	return func(tx *storage.WriteTx) error {
		cred, err := req.revalidate(tx)
		if err != nil {
			return err
		}
		if cred.Installation == nil {
			return fail(http.StatusForbidden, "forbidden", "Register this browser before importing a library.")
		}
		return nil
	}
}

func importValue(im storage.Import) map[string]any {
	v := map[string]any{"import_id": im.ImportID, "kind": im.Kind, "fingerprint": im.Fingerprint, "size": im.Size, "file_name": im.FileName,
		"status": im.Status, "created_at": im.CreatedAt, "updated_at": im.UpdatedAt}
	if im.Reason != "" {
		v["reason"] = im.Reason
	}
	if im.Result != "" {
		v["result"] = json.RawMessage(im.Result)
	}
	return v
}

func (req *request) reserveImport() (int, any, error) {
	r, err := req.importer()
	if err != nil {
		return 0, nil, err
	}
	var body struct {
		Kind        string `json:"kind"`
		Fingerprint string `json:"fingerprint"`
		Size        int64  `json:"size"`
		FileName    string `json:"file_name"`
	}
	if err := req.decode(&body); err != nil {
		return 0, nil, err
	}
	im, created, err := r.Reserve(req.ctx, req.authorize(), req.principal.accountID(), body.Kind, body.Fingerprint, body.Size, body.FileName)
	if err != nil {
		return 0, nil, importError(err)
	}
	status := http.StatusOK
	if created {
		status = http.StatusCreated
	}
	return status, importValue(im), nil
}

func (req *request) currentImport() (int, any, error) {
	r, err := req.importer()
	if err != nil {
		return 0, nil, err
	}
	im, err := r.Current(req.ctx, req.principal.accountID())
	// The caller is checked again just before the import's state is answered,
	// even its absence.
	if err := req.checkCaller(); err != nil {
		return 0, nil, err
	}
	if errors.Is(err, importer.ErrNotFound) {
		return http.StatusOK, map[string]any{"import": nil}, nil
	}
	if err != nil {
		return 0, nil, err
	}
	return http.StatusOK, map[string]any{"import": importValue(im)}, nil
}

func (req *request) getImport() (int, any, error) {
	r, err := req.importer()
	if err != nil {
		return 0, nil, err
	}
	im, err := r.Get(req.ctx, req.principal.accountID(), req.r.PathValue("id"))
	if err := req.checkCaller(); err != nil {
		return 0, nil, err
	}
	if err != nil {
		return 0, nil, importError(err)
	}
	return http.StatusOK, importValue(im), nil
}

func (req *request) importChunk() (int, any, error) {
	r, err := req.importer()
	if err != nil {
		return 0, nil, err
	}
	n, err := strconv.Atoi(req.r.PathValue("n"))
	if err != nil {
		return 0, nil, errNotFound
	}
	var body struct {
		Data string `json:"data"`
	}
	if err := req.decode(&body); err != nil {
		return 0, nil, err
	}
	data, err := base64.StdEncoding.DecodeString(body.Data)
	if err != nil {
		return 0, nil, invalid("A part must be base64.")
	}
	if err := r.Chunk(req.ctx, req.authorize(), req.principal.accountID(), req.r.PathValue("id"), n, data); err != nil {
		return 0, nil, importError(err)
	}
	return http.StatusOK, map[string]any{}, nil
}

func (req *request) finishImport() (int, any, error) {
	r, err := req.importer()
	if err != nil {
		return 0, nil, err
	}
	if err := req.decodeEmpty(); err != nil {
		return 0, nil, err
	}
	im, err := r.Finish(req.ctx, req.authorize(), req.principal.accountID(), req.r.PathValue("id"))
	if err != nil {
		return 0, nil, importError(err)
	}
	return http.StatusOK, importValue(im), nil
}
