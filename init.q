// 初始化的逻辑和每日更新的逻辑不同，初始化时需要考虑大批量文件IO的性能
// 考虑到TDX的文件以股票为索引，而kdb更适合以时间为索引，为避免反复IO文件又丢掉无用数据，可考虑先将全部文件原子化为【单日单股票】的过渡文件，然后再建库

slicepath:":D:/faca/inittmp/"
// ==================================1min  先拆分===============================
pathsz:":D:/work/tdx/vipdoc/sz/minline/"
pathsh:":D:/work/tdx/vipdoc/sh/minline/"
stock:"sh000001.lc1"
readtdx:{[x] ("hheeeeeii";2 2 4 4 4 4 4 4 4) 1: x}
ret: readtdx `$ pathsh,stock

fillzero: {[x] $[(count x)=2;x;"0",x]}
getyear: {[x] string (floor[x%2048])+2004}
getmonth: {[x] string (floor[mod[x;2048]%100])}
getday: {[x] string mod[mod[x;2048];100]}
casttodate: {[x] (getyear shortadj x),(fillzero getmonth shortadj x),(fillzero getday shortadj x)}
shortadj:{[x] $[x>0;x;65536+x]}  //short 类型太大时变成负值，这时需要+65536

szfilelist: key `$ pathsz  // 只保留指数和A股
szfilelist:szfilelist[where (`$ sublist[4]'[string szfilelist]) in `sz00`sz30`sz39]
szstocklist: "." vs' string szfilelist
getcode: {[x] x[0]}
szstocklist:`$ (getcode') szstocklist
addpath:{[x] `$ pathsz,(string x)}
sztosavestock: (addpath')[szfilelist]

shfilelist: key `$ pathsh  // 只保留指数和A股
shfilelist:shfilelist[where (`$ sublist[4]'[string shfilelist]) in `sh60`sh68`sh00`sh88`sh99]
shstocklist: "." vs' string shfilelist
shstocklist:`$ (getcode') shstocklist
addpath:{[x] `$ pathsh,(string x)}
shtosavestock: (addpath')[shfilelist]


gethour: {[x] string floor[x%60]}
getminute: {[x] string mod[x;60]}
casttotime: {[x] (gethour x),":",(fillzero getminute x)}

cutt:{[x;y] x[where x[;0]=\:y]}
getdates:{[x] (cutt[x]')distinct x[;0]}
makedtkey: {[x] flip(`$(casttodate')x[0]; "U"$(casttotime peach)x[1];x[2];x[3];x[4];x[5];x[6];x[7])}
readtdxfz:{[x] makedtkey[("hheeeeeii";2 2 4 4 4 4 4 4 4) 1: x]}

szsaveslice:{[x;y] (`$slicepath,(string szstocklist[y]),"/",(string x[0][0])) set x[;1 2 3 4 5 6 7]}  // 可以丢掉日期
szstockindex:til count sztosavestock
szmain:{[x](szsaveslice')[getdates readtdxfz sztosavestock x][x]}
shsaveslice:{[x;y] (`$slicepath,(string shstocklist[y]),"/",(string x[0][0])) set x[;1 2 3 4 5 6 7]}  // 可以丢掉日期
shstockindex:til count shtosavestock
shmain:{[x](shsaveslice')[getdates readtdxfz shtosavestock x][x]}
show .z.Z
(shmain peach)shstockindex
(szmain peach)szstockindex
show .z.Z
// ==================================1min  再聚合转存===============================
pathsz:":D:/work/tdx/vipdoc/sz/minline/"
pathsh:":D:/work/tdx/vipdoc/sh/minline/"
path2:":D:/faca/db/"
stock:"sh000001.lc1"
readtdx:{[x] ("hheeeeeii";2 2 4 4 4 4 4 4 4) 1: x}
ret: readtdx `$ pathsh,stock

fillzero: {[x] $[(count x)=2;x;"0",x]}
getyear: {[x] string (floor[x%2048])+2004}
getmonth: {[x] string (floor[mod[x;2048]%100])}
getday: {[x] string mod[mod[x;2048];100]}
casttodate: {[x] (getyear shortadj x),(fillzero getmonth shortadj x),(fillzero getday shortadj x)}
shortadj:{[x] $[x>0;x;65536+x]}  //short 类型太大时变成负值，这时需要+65536
tdxdatelist: distinct `$ (casttodate') ret[0]

exist:{[x] not()~key x}
filllist:{[x;y] x}
stockcolumn:{[x;y](flip y) upsert `$(filllist[("/" vs string x)[3]]')[til count y]}
addpath:{[x] `$ slicepath,(string x),"/"}
tmppath:(addpath')(shstocklist , szstocklist)
concat: {[x](upsert/)x}
onedaypath:{[x]`$ ((string tmppath),\:)string tdxdatelist[x]}
getdailydata:{[x]stockcolumn[x; get x]}
indexx:{[x]x[where (exist')x]}
format:{[x] ([] sym:x[7]; time:x[0];open:x[1]; high:x[2]; low:x[3]; close:x[4]; amount:x[5]; volume:x[6])}
savepath:{[x] `$(path2,(string tdxdatelist  x),"/1m/")}
savetable:{[x] savepath[x] set .Q.en[`:D:/faca/db] format concat (getdailydata')indexx onedaypath x}
onedaypathX:{[x]`$ ((string tmppath),\:)(string tdxdatelist[x]),"#"}  // 带#文件
deletefiles:{[x] (hdel') indexx onedaypath x; (hdel') indexx onedaypathX x}
saveanddel:{[x] savetable x; deletefiles x}
(saveanddel peach) til count tdxdatelist
