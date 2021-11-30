package migration

import (
	"database/sql"
	"fmt"
	"sort"
	"strings"
)

type Type int

const (
	DDL Type = 1 << iota
	DML
	ALL = DDL | DML
)

type Info struct {
	Version string
	Remark  string
}

type SqlItem struct {
	template string
	vars     []interface{}
	preview  string
}

type Drive interface {
	Drive() string
}

type Search interface {
	FindOne(sqlTemplate string, values ...interface{}) (*sql.Row, error)
	FindAll(sqlTemplate string, values ...interface{}) (*sql.Rows, error)
}

type Database interface {
	Search
	Drive
	GetSQL(sqlTemplate string, values ...interface{}) SqlItem
	LastRunVersion() string
	Exec(sqlTemplate string, values ...interface{}) error
	Rollback()
	Commit() error
}

type Option interface {
	Search
	Drive
	Exec(sqlType Type, sqlTemplate string, values ...interface{})
}

type Script interface {
	Up()
	Down()
	GetInfo() Info
	SetOption(opt Option) Script
}

type Migration interface {
	Drive
	// Run 在事务中执行SQL
	Run(sqlType Type) Migration
	// Commit 提交事务
	Commit() Migration
	// AddScript 添加版本
	AddScript(version string, s Script) Migration
	// PrepareUp 执行到指定版本的脚本
	PrepareUp(targetVersion string) Migration
	// PrepareDown 回滚到指定版本的脚本
	PrepareDown(targetVersion string) Migration
	// PreparePatchUp 选择特定的`PATCH.`开头的脚本执行
	PreparePatchUp(targetVersion string) Migration
	// PreparePatchDown 选择特定的`PATCH.`开头的脚本回滚
	PreparePatchDown(targetVersion string) Migration
}

func New(db Database) Migration {
	return &migration{db: db, ddlSqlList: []SqlItem{}, dmlSqlList: []SqlItem{}, script: map[string]Script{}}
}

type migration struct {
	db         Database
	ddlSqlList []SqlItem
	dmlSqlList []SqlItem
	script     map[string]Script
}

func (d migration) getPreviewSQL() string {
	result := ""
	if len(d.ddlSqlList) > 0 {
		result += "===== DDL SQL EXEC PREVIEW START ====="
		for i, item := range d.ddlSqlList {
			result = fmt.Sprintf("%v\n%v: %v", result, i+1, item.preview)
		}
		result = fmt.Sprintf("%v\nTOTAL: %v\n=====  DDL SQL EXEC PREVIEW END  =====", result, len(d.ddlSqlList))
	}
	if len(d.dmlSqlList) > 0 {
		result = fmt.Sprintf("%v\n===== DML SQL EXEC PREVIEW START =====", result)
		for i, item := range d.dmlSqlList {
			result = fmt.Sprintf("%v\n%v: %v", result, i+1, item.preview)
		}
		result = fmt.Sprintf("%v\nTOTAL: %v\n=====  DML SQL EXEC PREVIEW END  =====", result, len(d.dmlSqlList))
	}
	return result
}

func (d migration) String() string {
	return d.getPreviewSQL()
}

func (d *migration) Run(sqlType Type) Migration {
	defer func() {
		if r := recover(); r != nil {
			d.db.Rollback()
			panic(r)
		}
	}()
	if (sqlType & DDL) > 0 {
		for _, item := range d.ddlSqlList {
			if err := d.db.Exec(item.template, item.vars...); err != nil {
				panic(fmt.Errorf("pushSql error: %w. the raw error sql: %v", err, item.preview))
			}
		}
	}
	if (sqlType & DML) > 0 {
		for _, item := range d.dmlSqlList {
			if err := d.db.Exec(item.template, item.vars...); err != nil {
				panic(fmt.Errorf("pushSql error: %w. the raw error sql: %v", err, item.preview))
			}
		}
	}
	return d
}

func (d *migration) Commit() Migration {
	defer func() {
		if r := recover(); r != nil {
			d.db.Rollback()
			panic(r)
		}
	}()
	if err := d.db.Commit(); err != nil {
		panic(fmt.Errorf("commit error: %w", err))
	}
	return d
}

func (d *migration) AddScript(version string, s Script) Migration {
	d.script[version] = s
	return d
}

func (d *migration) PrepareUp(targetVersion string) Migration {
	currentVersion := d.db.LastRunVersion()
	selectVersion := getVersionSpan(d.script, currentVersion, targetVersion)
	sort.Strings(selectVersion)
	o := &option{d}
	for _, version := range selectVersion {
		d.script[version].SetOption(o).Up()
	}
	return d
}

func (d *migration) PrepareDown(targetVersion string) Migration {
	currentVersion := d.db.LastRunVersion()
	selectVersion := getVersionSpan(d.script, targetVersion, currentVersion)
	sort.Sort(sort.Reverse(sort.StringSlice(selectVersion)))
	o := &option{d}
	for _, version := range selectVersion {
		d.script[version].SetOption(o).Down()
	}
	return d
}

func (d *migration) PreparePatchUp(targetVersion string) Migration {
	d.getPatchScript(targetVersion).Up()
	return d
}

func (d *migration) PreparePatchDown(targetVersion string) Migration {
	d.getPatchScript(targetVersion).Down()
	return d
}

func (d *migration) Drive() string {
	return d.db.Drive()
}

func (d *migration) getPatchScript(targetVersion string) Script {
	if !strings.HasPrefix(targetVersion, "PATCH.") {
		panic("patch migration must has `PATCH.` prefix")
	}
	script, ok := d.script[targetVersion]
	if !ok || targetVersion != script.GetInfo().Version {
		panic("the version config must equal script version")
	}
	return script
}

func getVersionSpan(s map[string]Script, minVersion string, maxVersion string) []string {
	selectVersion := make([]string, 0, 16)
	for ver, script := range s {
		if ver != script.GetInfo().Version {
			panic("the version config must equal script version")
		}
		if strings.HasPrefix(ver, "PATCH.") || ver <= minVersion || ver > maxVersion {
			continue
		}
		selectVersion = append(selectVersion, ver)
	}
	return selectVersion
}

type option struct {
	m *migration
}

func (d *option) Exec(sqlType Type, sqlTemplate string, values ...interface{}) {
	sqlRaw := d.m.db.GetSQL(sqlTemplate, values...)
	switch sqlType {
	case DDL:
		d.m.ddlSqlList = append(d.m.ddlSqlList, sqlRaw)
	case DML:
		d.m.dmlSqlList = append(d.m.dmlSqlList, sqlRaw)
	default:
		panic("migration only run DDL or DML SQL")
	}
}

func (d *option) FindOne(sqlTemplate string, values ...interface{}) (*sql.Row, error) {
	return d.m.db.FindOne(sqlTemplate, values...)
}

func (d *option) FindAll(sqlTemplate string, values ...interface{}) (*sql.Rows, error) {
	return d.m.db.FindAll(sqlTemplate, values...)
}

func (d *option) Drive() string {
	return d.m.db.Drive()
}
