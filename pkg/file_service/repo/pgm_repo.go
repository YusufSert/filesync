package repo

import (
	"context"
	"database/sql"
	"fmt"
	"log/slog"

	_ "github.com/sijms/go-ora/v2"
)

type PGMRepo struct {
	db *sql.DB
	l  *slog.Logger
}

func NewPGMRepo(connStr string) (*PGMRepo, error) {
	db, err := sql.Open("oracle", connStr)
	if err != nil {
		return nil, fmt.Errorf("file_service-repo: could not open the connection %s %w", connStr, err)
	}
	if err = db.Ping(); err != nil {
		return nil, fmt.Errorf("file_service-repo: could not connect to db %s %w", connStr, err)
	}

	return &PGMRepo{db: db}, nil
}

func (r *PGMRepo) WriteDB(ctx context.Context, fileName, fileFolder string) error {
	q := "begin crm.pgm_pkg.FTP_INSERT(:p_gelen_dosya_adi, :p_gelen_dosya_klasor); end;" // note write begin and end lowercase
	stmt, err := r.db.Prepare(q)
	if err != nil {
		return fmt.Errorf("file_service-repo: could not prepare the statement for inserting to db %w", err)
	}

	_, err = stmt.ExecContext(ctx, fileName, fileFolder)
	if err != nil {
		return fmt.Errorf("file_service-repo: could not execute the statement for inserting to db p1=%s p2=%s %w", fileName, fileFolder, err) // todo: log params too
	}
	defer stmt.Close()
	return nil
}

func (r *PGMRepo) UpdateDB(ctx context.Context, fileName string, status int) error {
	q := "begin crm.pgm_pkg.FTP_EVRAK_STATUSUPDATE(:p_GIDEN_DOSYA, :p_evst_evst_id); end;"
	stmt, err := r.db.Prepare(q)
	if err != nil {
		return fmt.Errorf("file_service-repo: could not prepare the statement for updating file status %w", err)
	}

	_, err = stmt.ExecContext(ctx, fileName, status)
	if err != nil {
		return fmt.Errorf("file_service-repo: could not execute the statement for updating file status p1=%s p2=%d %w", fileName, status, err)
	}
	defer stmt.Close()
	return nil
}
