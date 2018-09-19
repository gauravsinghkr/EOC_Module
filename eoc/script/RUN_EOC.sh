convertPath() {
  cd $1
  origPath=`pwd -P`
  echo $origPath
}

BASEDIR=`cd $(dirname $0); pwd`
BASEDIR=`convertPath "$BASEDIR"`

cd $BASEDIR

HOSTNAME=`hostname`

echo ===============================================================================
echo .
echo   EOC Runner
echo .
echo   ReportDate $1
echo .
echo   IOId $2
echo .
echo   StartDate $3
echo .
echo   FilePath $4
echo .
echo   BASEDIR: $BASEDIR
echo .
echo   HOSTNAME: $HOSTNAME
echo .
echo ===============================================================================

exec /usr/bin/python Main_EOC.py $1 $2 $3 $4
