package migration

import (
	"database/sql"
	"strings"

	"gorm.io/driver/postgres"
	"gorm.io/driver/sqlite"
	"gorm.io/gorm"
)

type gormConn struct {
	demoDb *gorm.DB
	rwDb   *gorm.DB
}

func (g *gormConn) GetSQL(sqlTemplate string, values ...interface{}) SqlItem {
	stmt := g.demoDb.Exec(sqlTemplate, values...).Statement
	return SqlItem{
		template: stmt.SQL.String(),
		vars:     stmt.Vars,
		preview:  replaceAll(replaceAll(g.demoDb.Dialector.Explain(stmt.SQL.String(), stmt.Vars...), "\n"), "\t"),
	}
}

func (g *gormConn) FindOne(sqlTemplate string, values ...interface{}) (*sql.Row, error) {
	row := g.rwDb.Raw(sqlTemplate, values...).Row()
	return row, row.Err()
}

func (g *gormConn) FindAll(sqlTemplate string, values ...interface{}) (*sql.Rows, error) {
	return g.rwDb.Raw(sqlTemplate, values...).Rows()
}

func (g *gormConn) LastRunVersion() string {
	// TODO: code write here
	return "0.0.0"
}

func (g *gormConn) Exec(sqlTemplate string, values ...interface{}) error {
	db := g.rwDb.Exec(sqlTemplate, values...)
	return db.Error
}

func (g *gormConn) Rollback() {
	g.rwDb.Rollback()
}

func (g *gormConn) Commit() error {
	db := g.rwDb.Commit()
	return db.Error
}

func (g *gormConn) Drive() string {
	return g.rwDb.Dialector.Name()
}

func NewGorm(drive, dsn string) *gormConn {
	var d gorm.Dialector
	switch drive {
	case "postgres":
		d = postgres.Open(dsn)
	case "sqlite":
		d = sqlite.Open(dsn)
	default:
		panic("only support postgres or sqlite")
	}
	db, err := gorm.Open(d, &gorm.Config{})
	if err != nil {
		panic("failed to connect database")
	}
	return &gormConn{
		demoDb: db.Session(&gorm.Session{DryRun: true}),
		rwDb:   db.Session(&gorm.Session{}).Begin(),
	}
}

func replaceAll(old, prefix string) string {
	return strings.Replace(old, prefix, "", -1)
}
