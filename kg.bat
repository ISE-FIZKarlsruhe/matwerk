@echo off
REM Set paths
set COMPONENTSDIR=data/components
set REASONER=data/components/reasoner
set SRC=ontology/mwo-full.owl
set OUT=components/people.owl
set ROBOT_IMAGE=obolibrary/robot
set ONTBASE=https://nfdi.fiz-karlsruhe.de/ontology

REM Create components directory if it doesn't exist
if not exist %COMPONENTSDIR% (
    mkdir %COMPONENTSDIR%
)

echo Downloading the req_1 req_2
curl -L "https://docs.google.com/spreadsheets/d/e/2PACX-1vT-wK5CmuPc5ZXyNybym28yJPJ9z2H51Ry2SvWs4DXc_HcgwqRHOwdrz0oFhr9_D1MOxvGZS-Wb3YQE/pub?gid=394894036&single=true&output=tsv" -o %COMPONENTSDIR%\req_1.tsv
curl -L "https://docs.google.com/spreadsheets/d/e/2PACX-1vT-wK5CmuPc5ZXyNybym28yJPJ9z2H51Ry2SvWs4DXc_HcgwqRHOwdrz0oFhr9_D1MOxvGZS-Wb3YQE/pub?gid=0&single=true&output=tsv" -o %COMPONENTSDIR%\req_2.tsv

echo Downloading the agent role process
curl -L "https://docs.google.com/spreadsheets/d/e/2PACX-1vT-wK5CmuPc5ZXyNybym28yJPJ9z2H51Ry2SvWs4DXc_HcgwqRHOwdrz0oFhr9_D1MOxvGZS-Wb3YQE/pub?gid=2077140060&single=true&output=tsv" -o %COMPONENTSDIR%\agent.tsv
curl -L "https://docs.google.com/spreadsheets/d/e/2PACX-1vT-wK5CmuPc5ZXyNybym28yJPJ9z2H51Ry2SvWs4DXc_HcgwqRHOwdrz0oFhr9_D1MOxvGZS-Wb3YQE/pub?gid=1425127117&single=true&output=tsv" -o %COMPONENTSDIR%\role.tsv
curl -L "https://docs.google.com/spreadsheets/d/e/2PACX-1vT-wK5CmuPc5ZXyNybym28yJPJ9z2H51Ry2SvWs4DXc_HcgwqRHOwdrz0oFhr9_D1MOxvGZS-Wb3YQE/pub?gid=1169992315&single=true&output=tsv" -o %COMPONENTSDIR%\process.tsv

echo Downloading the city people organization
curl -L "https://docs.google.com/spreadsheets/d/e/2PACX-1vT-wK5CmuPc5ZXyNybym28yJPJ9z2H51Ry2SvWs4DXc_HcgwqRHOwdrz0oFhr9_D1MOxvGZS-Wb3YQE/pub?gid=1469482382&single=true&output=tsv" -o %COMPONENTSDIR%\city.tsv
curl -L "https://docs.google.com/spreadsheets/d/e/2PACX-1vT-wK5CmuPc5ZXyNybym28yJPJ9z2H51Ry2SvWs4DXc_HcgwqRHOwdrz0oFhr9_D1MOxvGZS-Wb3YQE/pub?gid=1666156492&single=true&output=tsv" -o %COMPONENTSDIR%\people.tsv
curl -L "https://docs.google.com/spreadsheets/d/e/2PACX-1vT-wK5CmuPc5ZXyNybym28yJPJ9z2H51Ry2SvWs4DXc_HcgwqRHOwdrz0oFhr9_D1MOxvGZS-Wb3YQE/pub?gid=447157523&single=true&output=tsv" -o %COMPONENTSDIR%\organization.tsv


echo Downloading the dataset
curl -L "https://docs.google.com/spreadsheets/d/e/2PACX-1vT-wK5CmuPc5ZXyNybym28yJPJ9z2H51Ry2SvWs4DXc_HcgwqRHOwdrz0oFhr9_D1MOxvGZS-Wb3YQE/pub?gid=1079878268&single=true&output=tsv" -o %COMPONENTSDIR%\dataset.tsv

echo Downloading the publication software dataportal
curl -L "https://docs.google.com/spreadsheets/d/e/2PACX-1vT-wK5CmuPc5ZXyNybym28yJPJ9z2H51Ry2SvWs4DXc_HcgwqRHOwdrz0oFhr9_D1MOxvGZS-Wb3YQE/pub?gid=1747331228&single=true&output=tsv" -o %COMPONENTSDIR%\publication.tsv
curl -L "https://docs.google.com/spreadsheets/d/e/2PACX-1vT-wK5CmuPc5ZXyNybym28yJPJ9z2H51Ry2SvWs4DXc_HcgwqRHOwdrz0oFhr9_D1MOxvGZS-Wb3YQE/pub?gid=1275685399&single=true&output=tsv" -o %COMPONENTSDIR%\software.tsv
curl -L "https://docs.google.com/spreadsheets/d/e/2PACX-1vT-wK5CmuPc5ZXyNybym28yJPJ9z2H51Ry2SvWs4DXc_HcgwqRHOwdrz0oFhr9_D1MOxvGZS-Wb3YQE/pub?gid=923160190&single=true&output=tsv" -o %COMPONENTSDIR%\dataportal.tsv

echo Downloading the instrument largescalefacility
curl -L "https://docs.google.com/spreadsheets/d/e/2PACX-1vT-wK5CmuPc5ZXyNybym28yJPJ9z2H51Ry2SvWs4DXc_HcgwqRHOwdrz0oFhr9_D1MOxvGZS-Wb3YQE/pub?gid=2015927839&single=true&output=tsv" -o %COMPONENTSDIR%\instrument.tsv
curl -L "https://docs.google.com/spreadsheets/d/e/2PACX-1vT-wK5CmuPc5ZXyNybym28yJPJ9z2H51Ry2SvWs4DXc_HcgwqRHOwdrz0oFhr9_D1MOxvGZS-Wb3YQE/pub?gid=370181939&single=true&output=tsv" -o %COMPONENTSDIR%\largescalefacility.tsv

echo Downloading the metadata ontology
curl -L "https://docs.google.com/spreadsheets/d/e/2PACX-1vT-wK5CmuPc5ZXyNybym28yJPJ9z2H51Ry2SvWs4DXc_HcgwqRHOwdrz0oFhr9_D1MOxvGZS-Wb3YQE/pub?gid=278046522&single=true&output=tsv" -o %COMPONENTSDIR%\metadata.tsv
::curl -L "https://docs.google.com/spreadsheets/d/e/2PACX-1vT-wK5CmuPc5ZXyNybym28yJPJ9z2H51Ry2SvWs4DXc_HcgwqRHOwdrz0oFhr9_D1MOxvGZS-Wb3YQE/pub?gid=2006199416&single=true&output=tsv" -o %COMPONENTSDIR%\ontology.tsv

echo Downloading the matwerkta matwerkiuc matwerkpp
curl -L "https://docs.google.com/spreadsheets/d/e/2PACX-1vT-wK5CmuPc5ZXyNybym28yJPJ9z2H51Ry2SvWs4DXc_HcgwqRHOwdrz0oFhr9_D1MOxvGZS-Wb3YQE/pub?gid=1489640604&single=true&output=tsv" -o %COMPONENTSDIR%\matwerkta.tsv
curl -L "https://docs.google.com/spreadsheets/d/e/2PACX-1vT-wK5CmuPc5ZXyNybym28yJPJ9z2H51Ry2SvWs4DXc_HcgwqRHOwdrz0oFhr9_D1MOxvGZS-Wb3YQE/pub?gid=281962521&single=true&output=tsv" -o %COMPONENTSDIR%\matwerkiuc.tsv
curl -L "https://docs.google.com/spreadsheets/d/e/2PACX-1vT-wK5CmuPc5ZXyNybym28yJPJ9z2H51Ry2SvWs4DXc_HcgwqRHOwdrz0oFhr9_D1MOxvGZS-Wb3YQE/pub?gid=606786541&single=true&output=tsv" -o %COMPONENTSDIR%\matwerkpp.tsv

echo Downloading the temporal event collaboration
curl -L "https://docs.google.com/spreadsheets/d/e/2PACX-1vT-wK5CmuPc5ZXyNybym28yJPJ9z2H51Ry2SvWs4DXc_HcgwqRHOwdrz0oFhr9_D1MOxvGZS-Wb3YQE/pub?gid=1265818056&single=true&output=tsv" -o %COMPONENTSDIR%\temporal.tsv
curl -L "https://docs.google.com/spreadsheets/d/e/2PACX-1vT-wK5CmuPc5ZXyNybym28yJPJ9z2H51Ry2SvWs4DXc_HcgwqRHOwdrz0oFhr9_D1MOxvGZS-Wb3YQE/pub?gid=638946284&single=true&output=tsv" -o %COMPONENTSDIR%\event.tsv
curl -L "https://docs.google.com/spreadsheets/d/e/2PACX-1vT-wK5CmuPc5ZXyNybym28yJPJ9z2H51Ry2SvWs4DXc_HcgwqRHOwdrz0oFhr9_D1MOxvGZS-Wb3YQE/pub?gid=266847052&single=true&output=tsv" -o %COMPONENTSDIR%\collaboration.tsv

echo Downloading the service
curl -L "https://docs.google.com/spreadsheets/d/e/2PACX-1vT-wK5CmuPc5ZXyNybym28yJPJ9z2H51Ry2SvWs4DXc_HcgwqRHOwdrz0oFhr9_D1MOxvGZS-Wb3YQE/pub?gid=130394813&single=true&output=tsv" -o %COMPONENTSDIR%\service.tsv

echo running the ROBOT for requirements
docker run --rm -v %cd%:/work -w /work -e "ROBOT_JAVA_ARGS=-Xmx8G -Dfile.encoding=UTF-8" ^
    %ROBOT_IMAGE% robot merge -i %SRC% template --template %COMPONENTSDIR%/req_1.tsv --prefix "nfdicore: https://nfdi.fiz-karlsruhe.de/ontology/" --output %COMPONENTSDIR%/req_1.owl
docker run --rm -v %cd%:/work -w /work -e "ROBOT_JAVA_ARGS=-Xmx8G -Dfile.encoding=UTF-8" ^
    %ROBOT_IMAGE% robot merge -i %SRC% -i %COMPONENTSDIR%/req_1.owl template --template %COMPONENTSDIR%/req_2.tsv --prefix "nfdicore: https://nfdi.fiz-karlsruhe.de/ontology/" --output %COMPONENTSDIR%/req_2.owl

echo running the ROBOT for agent
docker run --rm -v %cd%:/work -w /work -e "ROBOT_JAVA_ARGS=-Xmx8G -Dfile.encoding=UTF-8" ^
    %ROBOT_IMAGE% robot merge -i %SRC% -i %COMPONENTSDIR%/req_2.owl template --template %COMPONENTSDIR%/agent.tsv --prefix "nfdicore: https://nfdi.fiz-karlsruhe.de/ontology/" --output %COMPONENTSDIR%/agent.owl
docker run --rm -v %cd%:/work -w /work -e "ROBOT_JAVA_ARGS=-Xmx8G -Dfile.encoding=UTF-8" ^
    %ROBOT_IMAGE% robot explain --reasoner hermit --input %COMPONENTSDIR%/agent.owl -M inconsistency --explanation %REASONER%/agent_inconsistency.md 

echo running the ROBOT for role
docker run --rm -v %cd%:/work -w /work -e "ROBOT_JAVA_ARGS=-Xmx8G -Dfile.encoding=UTF-8" ^
    %ROBOT_IMAGE% robot merge -i %SRC% -i %COMPONENTSDIR%/req_2.owl -i %COMPONENTSDIR%/agent.owl template --template %COMPONENTSDIR%/role.tsv --prefix "nfdicore: https://nfdi.fiz-karlsruhe.de/ontology/" --output %COMPONENTSDIR%/role.owl
docker run --rm -v %cd%:/work -w /work -e "ROBOT_JAVA_ARGS=-Xmx8G -Dfile.encoding=UTF-8" ^
    %ROBOT_IMAGE% robot explain --reasoner hermit --input %COMPONENTSDIR%/role.owl -M inconsistency --explanation %REASONER%/role_inconsistency.md 

echo running the ROBOT for process
docker run --rm -v %cd%:/work -w /work -e "ROBOT_JAVA_ARGS=-Xmx8G -Dfile.encoding=UTF-8" ^
    %ROBOT_IMAGE% robot merge -i %SRC% -i %COMPONENTSDIR%/req_2.owl -i %COMPONENTSDIR%/agent.owl -i %COMPONENTSDIR%/role.owl template --template %COMPONENTSDIR%/process.tsv --prefix "nfdicore: https://nfdi.fiz-karlsruhe.de/ontology/" --output %COMPONENTSDIR%/process.owl
docker run --rm -v %cd%:/work -w /work -e "ROBOT_JAVA_ARGS=-Xmx8G -Dfile.encoding=UTF-8" ^
    %ROBOT_IMAGE% robot explain --reasoner hermit --input %COMPONENTSDIR%/process.owl -M inconsistency --explanation %REASONER%/process_inconsistency.md 

echo running the ROBOT for city
docker run --rm -v %cd%:/work -w /work -e "ROBOT_JAVA_ARGS=-Xmx8G -Dfile.encoding=UTF-8" ^
    %ROBOT_IMAGE% robot merge -i %SRC% -i %COMPONENTSDIR%/req_1.owl -i %COMPONENTSDIR%/req_2.owl template --template %COMPONENTSDIR%/city.tsv --prefix "nfdicore: https://nfdi.fiz-karlsruhe.de/ontology/" --output %COMPONENTSDIR%/city.owl
docker run --rm -v %cd%:/work -w /work -e "ROBOT_JAVA_ARGS=-Xmx8G -Dfile.encoding=UTF-8" ^
    %ROBOT_IMAGE% robot explain --reasoner hermit --input %COMPONENTSDIR%/city.owl -M inconsistency --explanation %REASONER%/city_inconsistency.md 

echo running the ROBOT for people
docker run --rm -v %cd%:/work -w /work -e "ROBOT_JAVA_ARGS=-Xmx8G -Dfile.encoding=UTF-8" ^
    %ROBOT_IMAGE% robot merge -i %SRC% -i %COMPONENTSDIR%/req_1.owl -i %COMPONENTSDIR%/req_2.owl -i %COMPONENTSDIR%/organization.owl template --template %COMPONENTSDIR%/people.tsv --prefix "nfdicore: https://nfdi.fiz-karlsruhe.de/ontology/" --output %COMPONENTSDIR%/people.owl
docker run --rm -v %cd%:/work -w /work -e "ROBOT_JAVA_ARGS=-Xmx8G -Dfile.encoding=UTF-8" ^
    %ROBOT_IMAGE% robot explain --reasoner hermit --input %COMPONENTSDIR%/people.owl -M inconsistency --explanation %REASONER%/people_inconsistency.md 

echo running the ROBOT for organization
docker run --rm -v %cd%:/work -w /work -e "ROBOT_JAVA_ARGS=-Xmx8G -Dfile.encoding=UTF-8" ^
    %ROBOT_IMAGE% robot merge -i %SRC% -i %COMPONENTSDIR%/req_1.owl -i %COMPONENTSDIR%/req_2.owl -i %COMPONENTSDIR%/city.owl template --template %COMPONENTSDIR%/organization.tsv --prefix "nfdicore: https://nfdi.fiz-karlsruhe.de/ontology/" --output %COMPONENTSDIR%/organization.owl
docker run --rm -v %cd%:/work -w /work -e "ROBOT_JAVA_ARGS=-Xmx8G -Dfile.encoding=UTF-8" ^
    %ROBOT_IMAGE% robot explain --reasoner hermit --input %COMPONENTSDIR%/organization.owl -M inconsistency --explanation %REASONER%/organization_inconsistency.md 

echo running the ROBOT for dataset
docker run --rm -v %cd%:/work -w /work -e "ROBOT_JAVA_ARGS=-Xmx8G -Dfile.encoding=UTF-8" ^
    %ROBOT_IMAGE% robot merge -i %SRC% -i %COMPONENTSDIR%/req_1.owl -i %COMPONENTSDIR%/req_2.owl -i %COMPONENTSDIR%/organization.owl -i %COMPONENTSDIR%/agent.owl -i %COMPONENTSDIR%/role.owl -i %COMPONENTSDIR%/process.owl template --template %COMPONENTSDIR%/dataset.tsv --prefix "nfdicore: https://nfdi.fiz-karlsruhe.de/ontology/" --output %COMPONENTSDIR%/dataset.owl
docker run --rm -v %cd%:/work -w /work -e "ROBOT_JAVA_ARGS=-Xmx8G -Dfile.encoding=UTF-8" ^
    %ROBOT_IMAGE% robot explain --reasoner hermit --input %COMPONENTSDIR%/dataset.owl -M inconsistency --explanation %REASONER%/dataset_inconsistency.md 

echo running the ROBOT for publication
docker run --rm -v %cd%:/work -w /work -e "ROBOT_JAVA_ARGS=-Xmx8G -Dfile.encoding=UTF-8" ^
    %ROBOT_IMAGE% robot merge -i %SRC% -i %COMPONENTSDIR%/req_2.owl -i %COMPONENTSDIR%/agent.owl -i %COMPONENTSDIR%/process.owl template --template %COMPONENTSDIR%/publication.tsv --prefix "nfdicore: https://nfdi.fiz-karlsruhe.de/ontology/" --output %COMPONENTSDIR%/publication.owl
docker run --rm -v %cd%:/work -w /work -e "ROBOT_JAVA_ARGS=-Xmx8G -Dfile.encoding=UTF-8" ^
    %ROBOT_IMAGE% robot explain --reasoner hermit --input %COMPONENTSDIR%/publication.owl -M inconsistency --explanation %REASONER%/publication_inconsistency.md 

echo running the ROBOT for software
docker run --rm -v %cd%:/work -w /work -e "ROBOT_JAVA_ARGS=-Xmx8G -Dfile.encoding=UTF-8" ^
    %ROBOT_IMAGE% robot merge -i %SRC% -i %COMPONENTSDIR%/req_1.owl -i %COMPONENTSDIR%/req_2.owl -i %COMPONENTSDIR%/publication.owl -i %COMPONENTSDIR%/process.owl template --template %COMPONENTSDIR%/software.tsv --prefix "nfdicore: https://nfdi.fiz-karlsruhe.de/ontology/" --output %COMPONENTSDIR%/software.owl
docker run --rm -v %cd%:/work -w /work -e "ROBOT_JAVA_ARGS=-Xmx8G -Dfile.encoding=UTF-8" ^
    %ROBOT_IMAGE% robot explain --reasoner hermit --input %COMPONENTSDIR%/software.owl -M inconsistency --explanation %REASONER%/software_inconsistency.md 

echo running the ROBOT for dataportal
docker run --rm -v %cd%:/work -w /work -e "ROBOT_JAVA_ARGS=-Xmx8G -Dfile.encoding=UTF-8" ^
    %ROBOT_IMAGE% robot merge -i %SRC% -i %COMPONENTSDIR%/req_1.owl -i %COMPONENTSDIR%/req_2.owl -i %COMPONENTSDIR%/publication.owl -i %COMPONENTSDIR%/organization.owl -i %COMPONENTSDIR%/process.owl template --template %COMPONENTSDIR%/dataportal.tsv --prefix "nfdicore: https://nfdi.fiz-karlsruhe.de/ontology/" --output %COMPONENTSDIR%/dataportal.owl
docker run --rm -v %cd%:/work -w /work -e "ROBOT_JAVA_ARGS=-Xmx8G -Dfile.encoding=UTF-8" ^
    %ROBOT_IMAGE% robot explain --reasoner hermit --input %COMPONENTSDIR%/dataportal.owl -M inconsistency --explanation %REASONER%/dataportal_inconsistency.md 


echo running the ROBOT for instrument
docker run --rm -v %cd%:/work -w /work -e "ROBOT_JAVA_ARGS=-Xmx8G -Dfile.encoding=UTF-8" ^
    %ROBOT_IMAGE% robot merge -i %SRC% -i %COMPONENTSDIR%/req_1.owl -i %COMPONENTSDIR%/req_2.owl -i %COMPONENTSDIR%/publication.owl -i %COMPONENTSDIR%/organization.owl -i %COMPONENTSDIR%/agent.owl -i %COMPONENTSDIR%/role.owl -i %COMPONENTSDIR%/process.owl template --template %COMPONENTSDIR%/instrument.tsv --prefix "nfdicore: https://nfdi.fiz-karlsruhe.de/ontology/" --output %COMPONENTSDIR%/instrument.owl
docker run --rm -v %cd%:/work -w /work -e "ROBOT_JAVA_ARGS=-Xmx8G -Dfile.encoding=UTF-8" ^
    %ROBOT_IMAGE% robot explain --reasoner hermit --input %COMPONENTSDIR%/instrument.owl -M inconsistency --explanation %REASONER%/instrument_inconsistency.md 

echo running the ROBOT for largescalefacility
docker run --rm -v %cd%:/work -w /work -e "ROBOT_JAVA_ARGS=-Xmx8G -Dfile.encoding=UTF-8" ^
    %ROBOT_IMAGE% robot merge -i %SRC% -i %COMPONENTSDIR%/req_1.owl -i %COMPONENTSDIR%/req_2.owl -i %COMPONENTSDIR%/publication.owl -i %COMPONENTSDIR%/organization.owl -i %COMPONENTSDIR%/agent.owl -i %COMPONENTSDIR%/role.owl -i %COMPONENTSDIR%/process.owl template --template %COMPONENTSDIR%/largescalefacility.tsv --prefix "nfdicore: https://nfdi.fiz-karlsruhe.de/ontology/" --output %COMPONENTSDIR%/largescalefacility.owl
docker run --rm -v %cd%:/work -w /work -e "ROBOT_JAVA_ARGS=-Xmx8G -Dfile.encoding=UTF-8" ^
    %ROBOT_IMAGE% robot explain --reasoner hermit --input %COMPONENTSDIR%/largescalefacility.owl -M inconsistency --explanation %REASONER%/largescalefacility_inconsistency.md 


echo running the ROBOT for metadata
docker run --rm -v %cd%:/work -w /work -e "ROBOT_JAVA_ARGS=-Xmx8G -Dfile.encoding=UTF-8" ^
    %ROBOT_IMAGE% robot merge -i %SRC% -i %COMPONENTSDIR%/req_1.owl -i %COMPONENTSDIR%/req_2.owl -i %COMPONENTSDIR%/publication.owl -i %COMPONENTSDIR%/organization.owl -i %COMPONENTSDIR%/process.owl template --template %COMPONENTSDIR%/metadata.tsv --prefix "nfdicore: https://nfdi.fiz-karlsruhe.de/ontology/" --output %COMPONENTSDIR%/metadata.owl
docker run --rm -v %cd%:/work -w /work -e "ROBOT_JAVA_ARGS=-Xmx8G -Dfile.encoding=UTF-8" ^
    %ROBOT_IMAGE% robot explain --reasoner hermit --input %COMPONENTSDIR%/metadata.owl -M inconsistency --explanation %REASONER%/metadata_inconsistency.md 

::echo running the ROBOT for ontology
::docker run --rm -v %cd%:/work -w /work -e "ROBOT_JAVA_ARGS=-Xmx8G -Dfile.encoding=UTF-8" ^
::    %ROBOT_IMAGE% robot merge -i %SRC% -i %COMPONENTSDIR%/req_1.owl -i %COMPONENTSDIR%/req_2.owl -i %COMPONENTSDIR%/publication.owl -i %COMPONENTSDIR%/organization.owl -i %COMPONENTSDIR%/process.owl template --template %COMPONENTSDIR%/ontology.tsv --prefix "nfdicore: https://nfdi.fiz-karlsruhe.de/ontology/" --output %COMPONENTSDIR%/ontology.owl
::docker run --rm -v %cd%:/work -w /work -e "ROBOT_JAVA_ARGS=-Xmx8G -Dfile.encoding=UTF-8" ^
::    %ROBOT_IMAGE% robot explain --reasoner hermit --input %COMPONENTSDIR%/ontology.owl -M inconsistency --explanation %REASONER%/ontology_inconsistency.md 


echo running the ROBOT for matwerkta
docker run --rm -v %cd%:/work -w /work -e "ROBOT_JAVA_ARGS=-Xmx8G -Dfile.encoding=UTF-8" ^
    %ROBOT_IMAGE% robot merge -i %SRC% -i %COMPONENTSDIR%/req_1.owl -i %COMPONENTSDIR%/req_2.owl -i %COMPONENTSDIR%/organization.owl -i %COMPONENTSDIR%/agent.owl -i %COMPONENTSDIR%/role.owl -i %COMPONENTSDIR%/process.owl template --template %COMPONENTSDIR%/matwerkta.tsv --prefix "nfdicore: https://nfdi.fiz-karlsruhe.de/ontology/" --output %COMPONENTSDIR%/matwerkta.owl
docker run --rm -v %cd%:/work -w /work -e "ROBOT_JAVA_ARGS=-Xmx8G -Dfile.encoding=UTF-8" ^
    %ROBOT_IMAGE% robot explain --reasoner hermit --input %COMPONENTSDIR%/matwerkta.owl -M inconsistency --explanation %REASONER%/matwerkta_inconsistency.md 

echo running the ROBOT for matwerkiuc
docker run --rm -v %cd%:/work -w /work -e "ROBOT_JAVA_ARGS=-Xmx8G -Dfile.encoding=UTF-8" ^
    %ROBOT_IMAGE% robot merge -i %SRC% -i %COMPONENTSDIR%/req_1.owl -i %COMPONENTSDIR%/req_2.owl -i %COMPONENTSDIR%/matwerkta.owl -i %COMPONENTSDIR%/organization.owl -i %COMPONENTSDIR%/agent.owl -i %COMPONENTSDIR%/role.owl -i %COMPONENTSDIR%/process.owl template --template %COMPONENTSDIR%/matwerkiuc.tsv --prefix "nfdicore: https://nfdi.fiz-karlsruhe.de/ontology/" --output %COMPONENTSDIR%/matwerkiuc.owl
docker run --rm -v %cd%:/work -w /work -e "ROBOT_JAVA_ARGS=-Xmx8G -Dfile.encoding=UTF-8" ^
    %ROBOT_IMAGE% robot explain --reasoner hermit --input %COMPONENTSDIR%/matwerkiuc.owl -M inconsistency --explanation %REASONER%/matwerkiuc_inconsistency.md 

echo running the ROBOT for matwerkpp
docker run --rm -v %cd%:/work -w /work -e "ROBOT_JAVA_ARGS=-Xmx8G -Dfile.encoding=UTF-8" ^
    %ROBOT_IMAGE% robot merge -i %SRC% -i %COMPONENTSDIR%/req_1.owl -i %COMPONENTSDIR%/req_2.owl -i %COMPONENTSDIR%/matwerkta.owl -i %COMPONENTSDIR%/matwerkiuc.owl -i %COMPONENTSDIR%/organization.owl -i %COMPONENTSDIR%/agent.owl -i %COMPONENTSDIR%/role.owl -i %COMPONENTSDIR%/process.owl template --template %COMPONENTSDIR%/matwerkpp.tsv --prefix "nfdicore: https://nfdi.fiz-karlsruhe.de/ontology/" --output %COMPONENTSDIR%/matwerkpp.owl
docker run --rm -v %cd%:/work -w /work -e "ROBOT_JAVA_ARGS=-Xmx8G -Dfile.encoding=UTF-8" ^
    %ROBOT_IMAGE% robot explain --reasoner hermit --input %COMPONENTSDIR%/matwerkpp.owl -M inconsistency --explanation %REASONER%/matwerkpp_inconsistency.md 


echo running the ROBOT for temporal
docker run --rm -v %cd%:/work -w /work -e "ROBOT_JAVA_ARGS=-Xmx8G -Dfile.encoding=UTF-8" ^
    %ROBOT_IMAGE% robot merge -i %SRC% -i %COMPONENTSDIR%/req_1.owl -i %COMPONENTSDIR%/req_2.owl -i %COMPONENTSDIR%/publication.owl -i %COMPONENTSDIR%/organization.owl -i %COMPONENTSDIR%/process.owl template --template %COMPONENTSDIR%/temporal.tsv --prefix "nfdicore: https://nfdi.fiz-karlsruhe.de/ontology/" --output %COMPONENTSDIR%/temporal.owl
docker run --rm -v %cd%:/work -w /work -e "ROBOT_JAVA_ARGS=-Xmx8G -Dfile.encoding=UTF-8" ^
    %ROBOT_IMAGE% robot explain --reasoner hermit --input %COMPONENTSDIR%/temporal.owl -M inconsistency --explanation %REASONER%/temporal_inconsistency.md 

echo running the ROBOT for event
docker run --rm -v %cd%:/work -w /work -e "ROBOT_JAVA_ARGS=-Xmx8G -Dfile.encoding=UTF-8" ^
    %ROBOT_IMAGE% robot merge -i %SRC% -i %COMPONENTSDIR%/req_1.owl -i %COMPONENTSDIR%/req_2.owl -i %COMPONENTSDIR%/organization.owl -i %COMPONENTSDIR%/temporal.owl -i %COMPONENTSDIR%/agent.owl -i %COMPONENTSDIR%/role.owl -i %COMPONENTSDIR%/process.owl template --template %COMPONENTSDIR%/event.tsv --prefix "nfdicore: https://nfdi.fiz-karlsruhe.de/ontology/" --output %COMPONENTSDIR%/event.owl
docker run --rm -v %cd%:/work -w /work -e "ROBOT_JAVA_ARGS=-Xmx8G -Dfile.encoding=UTF-8" ^
    %ROBOT_IMAGE% robot explain --reasoner hermit --input %COMPONENTSDIR%/event.owl -M inconsistency --explanation %REASONER%/event_inconsistency.md 

echo running the ROBOT for collaboration
docker run --rm -v %cd%:/work -w /work -e "ROBOT_JAVA_ARGS=-Xmx8G -Dfile.encoding=UTF-8" ^
    %ROBOT_IMAGE% robot merge -i %SRC% -i %COMPONENTSDIR%/req_1.owl -i %COMPONENTSDIR%/req_2.owl -i %COMPONENTSDIR%/temporal.owl -i %COMPONENTSDIR%/organization.owl -i %COMPONENTSDIR%/process.owl template --template %COMPONENTSDIR%/collaboration.tsv --prefix "nfdicore: https://nfdi.fiz-karlsruhe.de/ontology/" --output %COMPONENTSDIR%/collaboration.owl
docker run --rm -v %cd%:/work -w /work -e "ROBOT_JAVA_ARGS=-Xmx8G -Dfile.encoding=UTF-8" ^
    %ROBOT_IMAGE% robot explain --reasoner hermit --input %COMPONENTSDIR%/collaboration.owl -M inconsistency --explanation %REASONER%/collaboration_inconsistency.md 


echo running the ROBOT for service
docker run --rm -v %cd%:/work -w /work -e "ROBOT_JAVA_ARGS=-Xmx8G -Dfile.encoding=UTF-8" ^
    %ROBOT_IMAGE% robot merge -i %SRC% -i %COMPONENTSDIR%/req_1.owl -i %COMPONENTSDIR%/req_2.owl -i %COMPONENTSDIR%/organization.owl -i %COMPONENTSDIR%/temporal.owl -i %COMPONENTSDIR%/agent.owl -i %COMPONENTSDIR%/role.owl -i %COMPONENTSDIR%/process.owl template --template %COMPONENTSDIR%/service.tsv --prefix "nfdicore: https://nfdi.fiz-karlsruhe.de/ontology/" --output %COMPONENTSDIR%/service.owl
docker run --rm -v %cd%:/work -w /work -e "ROBOT_JAVA_ARGS=-Xmx8G -Dfile.encoding=UTF-8" ^
    %ROBOT_IMAGE% robot explain --reasoner hermit --input %COMPONENTSDIR%/service.owl -M inconsistency --explanation %REASONER%/service_inconsistency.md 


