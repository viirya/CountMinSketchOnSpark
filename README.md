
## Count-min sketch on Spark

Calculate the estimated frequencies by using count-min sketch algorithm described in [paper](http://dx.doi.org/10.1016/j.jalgor.2003.12.001) proposed by Cormode and Muthukrishnan.

## Build

    sbt/sbt assembly

## API

CountMinSketch object provides some APIs used to calculate the estimated frequencies of a column in a DataFrame.

    import org.viirya.CountMinSketch._

    private def toLetter(i: Int): String = (i + 97).toChar.toString

    // Generate a RDD
    val rows = Seq.tabulate(20) { i =>
      if (i % 3 == 0) (1, toLetter(1), -1.0) else (i, toLetter(i), i * -1.0)
    }

    // Create a DataFrame from the RDD
    val df = rows.toDF("numbers", "letters", "negDoubles")

    // Call API to calculate estimated frequencies of column "numbers"
    val results = countMinSketch(df, "numbers").collect()

The method `countMinSketch` will return a DataFrame which contains two columns. The first column is the column passed in `countMinSketch`. The second column named as first column name + "_freq", contains the estimated frequencies of the first column.



