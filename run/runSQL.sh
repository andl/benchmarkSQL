"$JAVA_HOME/bin/java" -cp ../target/*jar-with-dependencies.jar -Dprop=$1 -DcommandFile=$2 com.benchmarkSQL.ExecJDBC
