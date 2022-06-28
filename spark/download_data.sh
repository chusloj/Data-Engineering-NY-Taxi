set -e # quit if any non-zero code is returned

TAXI_TYPE=$1 # yellow
YEAR=$2 # 2021

# example link
# https://nyc-tlc.s3.amazonaws.com/trip+data/yellow_tripdata_2022-03.parquet

URL_PREFIX="https://nyc-tlc.s3.amazonaws.com/trip+data"

for MONTH in {1..12}; do
    FMONTH=`printf "%02d" ${MONTH}`
    # echo ${FMONTH}

    URL="${URL_PREFIX}/${TAXI_TYPE}_tripdata_${YEAR}-${FMONTH}.parquet" # there can be no whitespace in vairable assignment equal sign
    
    LOCAL_PREFIX="data/raw/${TAXI_TYPE}/${YEAR}/${FMONTH}"
    LOCAL_FILE="${TAXI_TYPE}_tripdata_${YEAR}_${FMONTH}.parquet"
    LOCAL_PATH="${LOCAL_PREFIX}/${LOCAL_FILE}"

    echo "downloading ${URL} to ${LOCAL_PATH}"
    mkdir -p ${LOCAL_PREFIX}
    curl -o ${LOCAL_PATH} ${URL}

    # echo "compressing ${LOCAL_PATH}"
    # gzip ${LOCAL_PATH}
done