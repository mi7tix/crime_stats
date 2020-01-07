

###############Execute with a spark frim CLI############


spark-submit --master local[*] --class org.brig.CrimeStats target/scala-2.11/crime-assembly-0.0.1.jar data/crime.csv data/offense_codes.csv data/out_fl
