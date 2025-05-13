#!/usr/bin/env bash


TIMEFORMAT=%R

RML_MAPPER=./rmlmapper-all.jar
PYRML_MAPPER=./pyrml-mapper.py

root=$(pwd)

KRUNS=10

content_rmlmapper=""
content_pyrml=""

for dir in $1/*
do
	if [[ -d $dir ]] && [[ $dir == *-$4 ]]; then
		rmlmapper=$records["rmlmapper"]
		
    	dir=${dir%*/}      # remove the trailing "/"
    	
    	echo "TESTCASE $dir"
    	
    	testcase_rm=${dir##*/} # print everything after the final "/"
    	testcase_py=$testcase_rm
    	
    	echo "TESTCASE $testcase_py" 
	    
	    declare -a rmlmapper_testcase
	    for i in $(seq 0 $((KRUNS-1)));
		do
		
			if [[ $dir == *SPARQL ]]; then
				rmlmapper_mapping_file="mapping-rmlmapper.ttl"
				pyrml_mapping_file="mapping-pyrml.ttl"
			elif [[ $dir == *SQL* ]]; then
				rmlmapper_mapping_file="mapping-rmlmapper.ttl"
				pyrml_mapping_file="mapping-pyrml.ttl"
				
				if [[ $i == 0 ]]; then
					cd $root 
					python3.9 sqlloader.py $testcase_py
				fi 
			else
				rmlmapper_mapping_file="mapping.ttl"
				pyrml_mapping_file="mapping.ttl"
			fi 
			cd $dir
			
			echo "RMLMapper"
			exec 3>&1 4>&2
			exectime=$( { time java -jar $RML_MAPPER -m $rmlmapper_mapping_file 1>&3 2>&4; } 2>&1 )
			exec 3>&- 4>&-
			testcase_rm="${testcase_rm},${exectime}"
			
			echo "PyRML"
			
			exec 3>&1 4>&2
			exectime=$( { time python3.9 $PYRML_MAPPER $pyrml_mapping_file 1>&3 2>&4; } 2>&1 )
			exec 3>&- 4>&-
			testcase_py="${testcase_py},${exectime}"
			
		done
		
		content_rmlmapper="${content_rmlmapper} ${testcase_rm}\n"
		content_pyrml="${content_pyrml} ${testcase_py}\n"		
	fi
done

cd $root
echo $content_rmlmapper > $2
echo $content_pyrml > $3
