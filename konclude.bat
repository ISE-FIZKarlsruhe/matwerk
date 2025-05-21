@echo on
REM Enable delayed expansion
setlocal enabledelayedexpansion

REM Set paths
set COMPONENTSDIR=data
set VALIDATIONSDIR=data/validation
set SRC=ontology/mwo-full.owl
set ROBOT_IMAGE=obolibrary/robot
set ONTBASE=https://nfdi.fiz-karlsruhe.de/ontology
set KONCLUDE_IMAGE=konclude/konclude

REM Reason with Konclude
echo Running Konclude reasoning...
docker run --rm -v %cd%:/data %KONCLUDE_IMAGE% realize ^
    -i /data/%COMPONENTSDIR%/all_NotReasoned.owl ^
    -o /data/%COMPONENTSDIR%/all.ttl

REM Merge inverse data with reasoned ontology
echo Merging inverse_all.ttl with all.ttl...
docker run --rm -v %cd%:/work -w /work -e "ROBOT_JAVA_ARGS=-Xmx8G -Dfile.encoding=UTF-8" ^
    %ROBOT_IMAGE% robot merge ^
    --input %COMPONENTSDIR%/all.ttl ^
    --input %COMPONENTSDIR%/inverse/inverse_all.ttl ^
    --output %COMPONENTSDIR%/all.ttl

python3 -m pyshacl  -s shapes/shape4.ttl %COMPONENTSDIR%/all.ttl > %VALIDATIONSDIR%/shape4.md

echo  no roles without bearers
python3 -m pyshacl  -s shapes/shape3.ttl %COMPONENTSDIR%/all.ttl > %VALIDATIONSDIR%/shape3.md

echo  no punning
python3 -m pyshacl  -s shapes/shape2.ttl %COMPONENTSDIR%/all.ttl  > %VALIDATIONSDIR%/shape2.md

echo  no orphaned textual entities
python3 -m pyshacl  -s shapes/shape1.ttl %COMPONENTSDIR%/all.ttl > %VALIDATIONSDIR%/shape1.md

docker run --rm -v %cd%:/work -w /work -e "ROBOT_JAVA_ARGS=-Xmx8G -Dfile.encoding=UTF-8" ^
    %ROBOT_IMAGE% robot verify --input %COMPONENTSDIR%/all.ttl --queries shapes/verify1.sparql --output-dir data/validation/ -vvv > %VALIDATIONSDIR%/verify1.md
