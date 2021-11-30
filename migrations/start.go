package migrations

import (
	"fmt"
	"github.com/mapoio/elune/library/database/migration"
	"gorm.io/driver/sqlite"
)

// 构建一个预执行SQL脚本，用于是否提交
// 顺序如下 推导出实际执行的SQL -> 执行脚本 -> 提交事务
// 整批成功/整批失败

func script() {
	orm := migration.NewGorm(sqlite.Open("test.db"))
	var m migration.Migration = migration.New(orm)
	m.AddScript("0.0.1", &v001{})
	m.PrepareUp("0.0.1")
	fmt.Println(m)
	m.Run(migration.ALL).Commit()
}

func Start() {
	script()
}
