"$JAVA_HOME/bin/java" -cp ../target/*jar-with-dependencies.jar  -Dprop=$1 com.benchmarkSQL.LoadData -log .  $2 $3 $4 $5
