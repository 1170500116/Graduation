#！bin/sh
func() {
    echo "func:"
    echo "9.sh [-n client_num] [-m value_len][-i iter][-v verbose]"
    exit -1
}
let client_num=1
let value_len=1024
let iter=1000
let verbose=0
while getopts 'n:l:i:v:' OPT; do
    case $OPT in
        n) client_num=$OPTARG;;
        l) value_len=$OPTARG;;
        i) iter=$OPTARG;;
        v) verbose=$OPTARG;;
        ?) func;;
    esac
done
cd ./mycode8.0
for((i=0;i<$client_num;i++));
do
    nohup ./client -l $value_len -i $iter -v $verbose > client$i.log 2>&1 &
    echo "cd ./mycode8.0&&./client -l $value_len -i $iter -v $verbose > client$i.log 2>&1 &"
done