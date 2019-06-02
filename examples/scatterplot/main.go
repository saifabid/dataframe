package main

import (
	"fmt"
	"os"

	"github.com/saifabid/dataframe"
	"gonum.org/v1/plot"
	"gonum.org/v1/plot/plotter"
	"gonum.org/v1/plot/plotutil"
	"gonum.org/v1/plot/vg"
)

func main() {
	p := "train.csv"
	df := dataframe.ReadCSV(p)
	df.Summary()
	topDF := df.Head(100)

	pl, err := plot.New()
	if err != nil {
		fmt.Println(err)
		os.Exit(1)
	}

	pl.Title.Text = "Plot"
	pl.X.Label.Text = "x"
	pl.Y.Label.Text = "SalePrice"

	var xys plotter.XYs
	ys := topDF.GetCol("SalePrice").ToFloat64().ToFloat64Go()
	xs := topDF.GetCol("LotArea").ToFloat64().ToFloat64Go()
	for i := 0; i < len(ys); i++ {
		xys = append(xys, plotter.XY{
			Y: ys[i],
			X: xs[i],
		})
	}
	err = plotutil.AddScatters(pl, xys)
	if err != nil {
		fmt.Println(err)
		os.Exit(1)
	}

	pl.Save(8*vg.Inch, 8*vg.Inch, "points.png")
}
