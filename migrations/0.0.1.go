package migrations

import (
	"github.com/mapoio/elune/library/database/migration"
)

type v001 struct {
	opt migration.Option
}

func (v *v001) Up() {
	v.opt.Exec(migration.DDL, `
	CREATE TABLE users (
		id integer,
		created_at datetime,
		updated_at datetime,
		deleted_at datetime,
		PRIMARY KEY (id)
	)`)
}

func (v *v001) Down() {
	v.opt.Exec(migration.DDL, `DROP TABLE users`)
}

func (v *v001) GetInfo() migration.Info {
	return migration.Info{
		Version: "0.0.1",
		Remark:  "test",
	}
}

func (v *v001) SetOption(opt migration.Option) migration.Script {
	v.opt = opt
	return v
}
