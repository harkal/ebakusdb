package main

import (
	"fmt"
	"os"

	"github.com/ebakus/ebakusdb"
	"github.com/urfave/cli/altsrc"
	cli "gopkg.in/urfave/cli.v1"
)

func infoCmd(c *cli.Context) error {
	db, err := ebakusdb.Open(c.String("dbpath"), 0, nil)
	if err != nil || db == nil {
		return err
	}
	defer db.Close()

	i := db.GetInfo()

	fmt.Println("  DB Info ")
	fmt.Println("=================================")
	fmt.Printf(" Path       : %s\n", i.Path)
	fmt.Printf(" Capacity   : %d\n", i.TotalCapacity)
	fmt.Printf(" Used       : %d (%.1f%%)\n", i.TotalUsed, float64(i.TotalUsed)/float64(i.TotalCapacity)*100.0)
	fmt.Printf(" Watermark  : %d (%.1f%%)\n", i.Watermark, float64(i.Watermark)/float64(i.TotalCapacity)*100.0)
	fmt.Printf(" Page size  : %d\n", i.PageSize)
	fmt.Println("=================================")

	db.PrintFreeChunks()

	return nil
}

func main() {
	app := cli.NewApp()
	app.Name = "EbakusDB Tool"
	app.Version = "0.0.1"
	app.Authors = []cli.Author{
		cli.Author{
			Name:  "Harry Kalogirou",
			Email: "harkal@nlogn.eu",
		},
	}
	app.Copyright = "(c) 2020 Ebakus Team"
	app.Usage = "Run in various modes depending on function mode"

	genericFlags := []cli.Flag{
		altsrc.NewStringFlag(cli.StringFlag{
			Name:  "dbpath",
			Usage: "The ebakusdb file to use",
			Value: "",
		}),
		altsrc.NewStringFlag(cli.StringFlag{
			Name:  "dbhost",
			Value: "localhost",
		}),
	}

	app.Commands = []cli.Command{
		{
			Name:    "info",
			Aliases: []string{"i"},
			Usage:   "Print db information",
			Flags:   genericFlags,
			Action:  infoCmd,
		},
	}

	app.Run(os.Args)
}
