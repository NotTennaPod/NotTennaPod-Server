package protocol

import (
	"context"
	"errors"

	"github.com/NotTennaPod/NotTennaPod-Server/internal/contract"
	"github.com/NotTennaPod/NotTennaPod-Server/internal/storage"
)

// AuthorizedSecretValue is SecretValue for a caller: authorize runs in the
// read that reads the sealed secret, so a reset, revocation or disable that
// landed after authentication wins.
func (s *Service) AuthorizedSecretValue(ctx context.Context, accountID, id string, authorize AuthorizeRead) (map[string]any, error) {
	var sealedBytes []byte
	err := s.Store.Read(ctx, func(r *storage.ReadTx) error {
		if authorize != nil {
			if _, err := authorize(r); err != nil {
				return err
			}
		}
		var err error
		sealedBytes, err = r.Account(accountID).Secret(id)
		return err
	})
	if errors.Is(err, storage.ErrNotFound) {
		return nil, protoErr(404, "not_found", "No such secret.")
	}
	if err != nil {
		return nil, err
	}
	plain, err := s.Box.Open(sealedBytes, []byte("secret\x00"+accountID+"\x00"+id))
	if err != nil {
		return nil, err
	}
	v, err := contract.DecodeJSON(plain)
	if err != nil {
		return nil, err
	}
	return v.(map[string]any), nil
}
