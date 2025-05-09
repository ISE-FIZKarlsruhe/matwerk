@echo off
REM Set paths
set COMPONENTSDIR=data
set SRC=ontology/mwo-full.owl
set ROBOT_IMAGE=obolibrary/robot
set ONTBASE=https://nfdi.fiz-karlsruhe.de/ontology

docker run --rm -v %cd%:/work -w /work -e "ROBOT_JAVA_ARGS=-Xmx8G -Dfile.encoding=UTF-8" ^
    %ROBOT_IMAGE% robot merge -i %SRC% --inputs "data/components/*.owl" --output %COMPONENTSDIR%/all_NotReasoned.owl

docker run --rm -v %cd%:/work -w /work -e "ROBOT_JAVA_ARGS=-Xmx8G -Dfile.encoding=UTF-8" ^
    %ROBOT_IMAGE% robot merge -i %SRC% --inputs "data/components/*.owl" --output %COMPONENTSDIR%/all_NotReasoned.ttl

docker run --rm -v %cd%:/work -w /work -e "ROBOT_JAVA_ARGS=-Xmx8G -Dfile.encoding=UTF-8" ^
    %ROBOT_IMAGE% robot explain --input %COMPONENTSDIR%/all_NotReasoned.ttl -M inconsistency --explanation %COMPONENTSDIR%/inconsistency.md

docker run --rm -v %cd%:/work -w /work -e "ROBOT_JAVA_ARGS=-Xmx8G -Dfile.encoding=UTF-8" ^
    %ROBOT_IMAGE% robot reason --input %COMPONENTSDIR%/all_NotReasoned.ttl --output %COMPONENTSDIR%/all.ttl

docker run --rm -v %cd%:/work -w /work -e "ROBOT_JAVA_ARGS=-Xmx8G -Dfile.encoding=UTF-8" ^
    %ROBOT_IMAGE% robot explain --reasoner hermit --input %SRC% -M inconsistency --explanation %COMPONENTSDIR%/inconsistency_mwo.md


docker run --rm -v %cd%:/work -w /work -e "ROBOT_JAVA_ARGS=-Xmx8G -Dfile.encoding=UTF-8" ^
    %ROBOT_IMAGE% robot explain --reasoner hermit --input %COMPONENTSDIR%/all.ttl -M inconsistency --explanation %COMPONENTSDIR%/inconsistency_hermit.md

docker run --rm -v %cd%:/work -w /work -e "ROBOT_JAVA_ARGS=-Xmx8G -Dfile.encoding=UTF-8" ^
    %ROBOT_IMAGE% robot reason --reasoner hermit --input %COMPONENTSDIR%/all.ttl --output %COMPONENTSDIR%/all_reasoned_hermit.ttl


python3 -m pyshacl  -s shapes/shape4.ttl %COMPONENTSDIR%/all.ttl > %COMPONENTSDIR%/shape4.md

echo  no roles without bearers
python3 -m pyshacl  -s shapes/shape3.ttl %COMPONENTSDIR%/all.ttl > %COMPONENTSDIR%/shape3.md

echo  no punning
python3 -m pyshacl  -s shapes/shape2.ttl %COMPONENTSDIR%/all.ttl  > %COMPONENTSDIR%/shape2.md

echo  no orphaned textual entities
python3 -m pyshacl  -s shapes/shape1.ttl %COMPONENTSDIR%/all.ttl > %COMPONENTSDIR%/shape1.md

docker run --rm -v %cd%:/work -w /work -e "ROBOT_JAVA_ARGS=-Xmx8G -Dfile.encoding=UTF-8" ^
    %ROBOT_IMAGE% robot verify --input %COMPONENTSDIR%/all.ttl --queries shapes/verify1.sparql -vvv > %COMPONENTSDIR%/verify1.md