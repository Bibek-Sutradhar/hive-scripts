#!/bin/sh

########################################
#Author : Verizon Team
#Date : 8-12-2021
#Version : Initial Draft V1.0
#Description: Validate Hive Tables between Source (On-prem) and Target (GCP).
# UC#1: Match the row count of each table and Publish Matched Table and Mismatched Table data in CSV format
# UC#2: If the tables does match in terms of row count then the data integrity need to be matched. Take sample 10 percent of data
# 		and validate the hashkey of the data. Generate a report if the data does not match.
# 
#History :
# 1.0	Initial Draft	8-12-2021
#########################################

#Property File declaration
file="./configuration.properties"
#Start Date/Time
startDate=$(date +'%m-%d-%Y:%R')

#Start date Time
echo "Script execution Start time -->> $startDate"

baseDir="/home/vagrant/verizon/$(date +'%m-%d-%Y')"
#baseDir="/c/Verizon"
sourceRowCountDir=$baseDir/sourceRowCountDir
targetRowCountDir=$baseDir/targetRowCountDir

#clear out the base directory for next run
rm -rf $baseDir/


#If the directory is not present then create the directory
[ -d $baseDir ] || mkdir -p $baseDir
[ -d $sourceRowCountDir ] || mkdir -p $sourceRowCountDir
[ -d $targetRowCountDir ] || mkdir -p $targetRowCountDir

#Get the source section data from property file
sourceData=$(sed  '/Source/,/Target/!d;/Target/q' $file|sed '1d;$d')
for line in $sourceData
do
    key=`echo $line | cut -d: -f1`
    value=`echo $line | cut -d: -f2`
    if [ "$key" = "DBName" ]; then
	export srcdbName=$value
    elif [ "$key" = "Host" ]; then
	export srcHostName=$value
    elif [ "$key" = "Port" ]; then
	export srcPort=$value
    fi
done
    
echo $srcdbName
echo $srcHostName
echo $srcPort

#Get the target section data from property file
targetData=$(sed  '/Target/,/Tables/!d;/Tables/q' $file|sed '1d;$d')
for line in $targetData
do
    key=`echo $line | cut -d: -f1`
    value=`echo $line | cut -d: -f2`
    if [ "$key" = "DBName" ]; then
	export trgdbName=$value
    elif [ "$key" = "Host" ]; then
	export trgHostName=$value
    elif [ "$key" = "Port" ]; then
	export trgPort=$value
    fi
done
    
echo $trgdbName
echo $trgHostName
echo $trgPort

#Get all table names from the property file
tables=$(sed  '/Tables/,/SampleDataPercentage/!d;/SampleDataPercentage/q' $file|sed '1d;$d')
#echo $tables
for line in $tables; do
	echo "Table from configuration file is :: $line"
done

#Get sample data percentage to compare
sampleDataPercentage=$(sed  '/SampleDataPercentage/,/END/!d;/END/q' $file|sed '1d;$d')
#echo $sampleDataPercentage

#Current Date/Time
currentDate=$(date +'%m-%d-%Y')

###############################################################################
#UC#1: Count the number of rows for a table in Source and Target database.
#Compare and generate TableMismatchedData_<Date>.csv and MatchedData_<Date>.csv 
###############################################################################

#Export source table data in a file 
for tbl in $tables; do
    echo $tbl
	#capture start time with date for each table
	setCommand=$(hive -S -e "set hive.cbo.enable=true;
				set hive.compute.query.using.stats=true;
				set hive.stats.fetch.column.stats=true;
				set hive.stats.fetch.partition.stats=true;
				set hive.stats.autogather=true;")
	res=$(hive -e "use $srcdbName; show partitions $tbl;")
	
	if [ ! -z "$res" ]; then
		#remove default hive partition
		#resp_temp=$(echo "$res" | sed '$d')
		#echo $resp_temp
		
		for line in $res; do
			key=`echo $line | cut -d= -f1`
			value=`echo $line | cut -d= -f2`
			value_temp=$(echo $value | sed "s/^/'/;s/$/'/")
			partitionData=$key"="$value_temp
			echo $tbl:$partitionData
			
			for pData in $partitionData; do
				count=$(hive -S -e "use $srcdbName; select count(*) from $tbl where $pData;")	
				echo "$tbl:$count:$pData" >> $sourceRowCountDir/$srcdbName"_"SourceTableResult"_"$currentDate"_"temp.txt
			done
		done
	else
		count=$(hive -S -e "use $srcdbName; SELECT count(*) from $tbl;")
		echo "$tbl:$count" >> $sourceRowCountDir/$srcdbName"_"SourceTableResult"_"$currentDate"_"temp.txt
	fi
done

#Export target table data in a file 
for tbl in $tables; do
    echo $tbl
	#capture start time with date for each table
	setCommand=$(hive -S -e "set hive.cbo.enable=true;
				set hive.compute.query.using.stats=true;
				set hive.stats.fetch.column.stats=true;
				set hive.stats.fetch.partition.stats=true;
				set hive.stats.autogather=true;")
	res=$(hive -e "use $trgdbName; show partitions $tbl;")
	
	if [ ! -z "$res" ]; then
		#remove default hive partition
		#resp_temp=$(echo "$res" | sed '$d')
		#echo $resp_temp
		
		for line in $res; do
			key=`echo $line | cut -d= -f1`
			value=`echo $line | cut -d= -f2`
			value_temp=$(echo $value | sed "s/^/'/;s/$/'/")
			partitionData=$key"="$value_temp
			echo $tbl:$partitionData
			
			for pData in $partitionData; do
				count=$(hive -S -e "use $trgdbName; select count(*) from $tbl where $pData;")	
				echo "$tbl:$count:$pData" >> $targetRowCountDir/$trgdbName"_"TargetTableResult"_"$currentDate"_"temp.txt
			done
		done
	else
		count=$(hive -S -e "use $trgdbName; SELECT count(*) from $tbl;")
		echo "$tbl:$count" >> $targetRowCountDir/$trgdbName"_"TargetTableResult"_"$currentDate"_"temp.txt
	fi
done

#Clean the output file for white spaces
sed -e "s/\r//g" < $sourceRowCountDir/$srcdbName"_"SourceTableResult"_"$currentDate"_"temp.txt > $sourceRowCountDir/$srcdbName"_"SourceTableResult"_"$currentDate.txt
sed -e "s/\r//g" < $targetRowCountDir/$trgdbName"_"TargetTableResult"_"$currentDate"_"temp.txt > $targetRowCountDir/$trgdbName"_"TargetTableResult"_"$currentDate.txt
rm -rf $sourceRowCountDir/$srcdbName"_"SourceTableResult"_"$currentDate"_"temp.txt
rm -rf $targetRowCountDir/$trgdbName"_"TargetTableResult"_"$currentDate"_"temp.txt

#Compare the two output files and generate the report file if there are any differences
diff --side-by-side --suppress-common-lines $sourceRowCountDir/$srcdbName"_"SourceTableResult"_"$currentDate.txt $targetRowCountDir/$trgdbName"_"TargetTableResult"_"$currentDate.txt > $baseDir/TableMismatchedData"_"$currentDate.txt
sed -e "s/\r//g" < $baseDir/TableMismatchedData"_"$currentDate.txt > $baseDir/TableMismatchedData"_"$currentDate"_"formatted.txt
sed 's/ \+/,/g' $baseDir/TableMismatchedData"_"$currentDate"_"formatted.txt >  $baseDir/TableMismatchedData"_"$currentDate.csv
sed -i '1i"Source Database","Target Database"' $baseDir/TableMismatchedData"_"$currentDate.csv
sed -i 's/|//' $baseDir/TableMismatchedData"_"$currentDate.csv
rm -rf $baseDir/TableMismatchedData"_"$currentDate.txt


#Compare the two output fiels and generate the report file for matching records
grep -Fxf $sourceRowCountDir/$srcdbName"_"SourceTableResult"_"$currentDate.txt $targetRowCountDir/$trgdbName"_"TargetTableResult"_"$currentDate.txt > $baseDir/MatchedData"_"$currentDate.txt
sed -e "s/\r//g" < $baseDir/MatchedData"_"$currentDate.txt > $baseDir/MatchedData"_"$currentDate"_"formatted.txt
sed 's/:\+/,/g' $baseDir/MatchedData"_"$currentDate"_"formatted.txt > $baseDir/MatchedData"_"$currentDate.csv
sed -i '1i"Table Name","Total Number of rows","Patition ID"' $baseDir/MatchedData"_"$currentDate.csv
rm -rf $baseDir/MatchedData"_"$currentDate.txt


###################################################################################################
#UC#2: Take all the tables which passed UC#1 and check for data integrity by checking their haskey.
#Compare and generate TableDataNotConsistent_<Date>.csv
###################################################################################################

#Get the sample data from hive table from source and target into respective folders
matchedRowCountFile=$baseDir/MatchedData"_"$currentDate"_"formatted.txt
echo $matchedRowCountFile

c=0
while IFS= read -r line;
do
    rowCountArray[$c]=$line
    #echo ${rowCountArray[$c]} >> rowCountArray.txt
    c=$(($c+1))
   
done < $matchedRowCountFile

echo "Total number of matched rows for each table are:: ${#rowCountArray[*]}"

for line in "${rowCountArray[@]}"
do
	echo "$line" >> lineOutput.txt
    tableName=`echo $line | cut -d: -f1`
    rowCount=`echo $line | cut -d: -f2`
	partition=`echo $line | cut -d: -f3`
	
	if [ -z "$partition" ]
	then
		echo "----inside if condition--------"
	    echo "matched table is:: $tableName"
		echo "matched row count is :: $rowCount"
		echo "matched partition is :: $partition"
		sourceTableDate=$(hive -S -e "use $srcdbName;
		     INSERT OVERWRITE LOCAL DIRECTORY '$sourceRowCountDir/tables' ROW FORMAT DELIMITED FIELDS TERMINATED BY ',' SELECT * from $tableName limit $sampleDataPercentage;")

		targetTableDate=$(hive -S -e "use $trgdbName;
		     INSERT OVERWRITE LOCAL DIRECTORY '$targetRowCountDir/tables' ROW FORMAT DELIMITED FIELDS TERMINATED BY ',' SELECT * from $tableName limit $sampleDataPercentage;")
	else
		echo "----inside else condition--------"
		echo "matched table is:: $tableName"
		echo "matched row count is :: $rowCount"
		echo "matched partition is :: $partition"
		sourceTableDate=$(hive -S -e "use $srcdbName;
		     INSERT OVERWRITE LOCAL DIRECTORY '$sourceRowCountDir/tables' ROW FORMAT DELIMITED FIELDS TERMINATED BY ',' SELECT * from $tableName where $partition limit $sampleDataPercentage;")

		targetTableDate=$(hive -S -e "use $trgdbName;
		     INSERT OVERWRITE LOCAL DIRECTORY '$targetRowCountDir/tables' ROW FORMAT DELIMITED FIELDS TERMINATED BY ',' SELECT * from $tableName where $partition limit $sampleDataPercentage;")
	fi
    
    #Generate the hashkey for 2 files
    sourceHashKey=$(md5sum $sourceRowCountDir/tables/000000_0 | cut -c 1-32)
    targteHashKey=$(md5sum $targetRowCountDir/tables/000000_0 | cut -c 1-32)
    
    echo $sourceHashKey
    echo $targetHashKey

    if [[ "$sourceHashKey" == "$targteHashKey" ]]
    then
 	echo "Table $tableName data is validated and is same."
    else
	echo "$tableName:$partition" >> $baseDir/TableDataNotConsistent"_"$currentDate.txt
    fi
done

if [ -e $baseDir/TableDataNotConsistent"_"$currentDate.txt ] 
then
	sed 's/:\+/,/g' $baseDir/TableDataNotConsistent"_"$currentDate.txt > $baseDir/TableDataNotConsistent"_"$currentDate.csv
	sed -i '1i"Table Name","Partition ID"' $baseDir/TableDataNotConsistent"_"$currentDate.csv
fi

#End Date/Time
endDate=$(date +'%m-%d-%Y:%R')

#End date Time
echo "Script execution End time -->> $endDate"
