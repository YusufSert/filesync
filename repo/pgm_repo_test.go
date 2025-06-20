package repo

import (
	"context"
	"fmt"
	"testing"
)

func TestPGMWrite(t *testing.T) {
	r, err := NewPGMRepo("oracle://tcysert:oracle_4U@172.26.20.89:1521/KAZANTST")
	if err != nil {
		t.Fatal(err) //fssss
	}

	ctx := context.Background()
	err = r.WriteDB(ctx, "GPM-03-1-6-25-8_x2761.QRP", "2026\\Gelen\\")
	if err != nil {
		t.Fatal(err)
	}
}

func TestPGMUpdate(t *testing.T) {
	r, err := NewPGMRepo("oracle://tcysert:oracle_4U@172.26.20.89:1521/KAZANTST")
	if err != nil {
		t.Fatal(err)
	}

	ctx := context.Background()
	err = r.UpdateDB(ctx, "GPM-03-1-6-25-8_x2761.QRP", 5)
	if err != nil {
		t.Fatal(err)
	}
	fmt.Println(err)
}
