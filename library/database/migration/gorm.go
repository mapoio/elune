package migration

import (
	"database/sql"
	"strings"

	"gorm.io/gorm"
)

type GormMigration struct {
	gorm.Model
	Version string
	Type    string
}

func (GormMigration) TableName() string {
	return "gorm_auto_migration_table_do_not_delete"
}

type gormConn struct {
	demoDb  *gorm.DB
	rwDb    *gorm.DB
	version *GormMigration
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

func (g *gormConn) SetMigrationVersion(version, sqlType string) {
	g.version = &GormMigration{
		Version: version,
		Type:    sqlType,
	}
}

func (g *gormConn) LastRunVersion() string {
	version := &GormMigration{
		Version: "0.0.0",
		Type:    "init",
	}
	g.rwDb.Last(version)
	return version.Version
}

func (g *gormConn) Exec(sqlTemplate string, values ...interface{}) error {
	db := g.rwDb.Exec(sqlTemplate, values...)
	return db.Error
}

func (g *gormConn) Rollback() {
	g.rwDb.Rollback()
}

func (g *gormConn) Commit() error {
	if g.version != nil {
		err := g.rwDb.Create(g.version).Error
		if err != nil {
			panic("create version error: " + err.Error())
		}
	}
	db := g.rwDb.Commit()
	return db.Error
}

func (g *gormConn) Drive() string {
	return g.rwDb.Dialector.Name()
}

func NewGorm(director gorm.Dialector) *gormConn {
	db, err := gorm.Open(director, &gorm.Config{})
	if err != nil {
		panic("connect database failed: " + err.Error())
	}
	err = db.AutoMigrate(&GormMigration{})
	if err != nil {
		panic("AutoMigrate failed: " + err.Error())
	}
	return &gormConn{
		demoDb: db.Session(&gorm.Session{DryRun: true}),
		rwDb:   db.Session(&gorm.Session{}).Begin(),
	}
}

func replaceAll(old, prefix string) string {
	return strings.Replace(old, prefix, "", -1)
}
