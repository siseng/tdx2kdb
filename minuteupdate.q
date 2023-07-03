// ==================================1min===============================
path2:":D:/faca/db/"
func1: {[x] type key `$ path2,(string x)}
filelist: key `$ path2
findall:{where x~\:y}
folderindex: findall[(func1')filelist; 11h]
folderlist:filelist[folderindex]
exist:{[x] not()~key x}
func2:{[x]exist `$ path2,x,"/1m"}
dbdatelist: folderlist[where (func2') string folderlist]

pathsz:":D:/work/tdx/vipdoc/sz/minline/"
pathsh:":D:/work/tdx/vipdoc/sh/minline/"
stock:"sh000001.lc1"
readtdx:{[x] ("hheeeeeii";2 2 4 4 4 4 4 4 4) 1: x}
ret: readtdx `$ pathsh,stock
droplist:{[x;y] x _ x?y}

fillzero: {[x] $[(count x)=2;x;"0",x]}
getyear: {[x] string (floor[x%2048])+2004}
getmonth: {[x] string (floor[mod[x;2048]%100])}
getday: {[x] string mod[mod[x;2048];100]}
casttodate: {[x] (getyear shortadj x),(fillzero getmonth shortadj x),(fillzero getday shortadj x)}
shortadj:{[x] $[x>0;x;65536+x]}  //short 类型太大时变成负值，这时需要+65536
tdxdatelist: distinct `$ (casttodate') ret[0]
tosavedate1m:droplist/[tdxdatelist;dbdatelist]

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
filllist:{[x;y] x}
stockcolumn: {[x;y] (filllist[x] peach)[til count y]}
makedtkey: {[x;y] flip(`$ (casttodate peach) x[0]; "U"$(casttotime peach)x[1];x[2];x[3];x[4];x[5];x[6];x[7]; stockcolumn[y; x[0]])}
filterdt: {[x;y]x[where x[;0]=y]}
readtdxfz:{[x;y;z] filterdt[makedtkey[("hheeeeeii";2 2 4 4 4 4 4 4 4) 1: x; y];z]} / x:file y:code z:date


szstockindex:til count sztosavestock
getszdatafz:{[y;x] readtdxfz[sztosavestock[x];szstocklist[x]; y]}
concat: {[x](upsert/)x}
format:{[x] ([] sym:x[8]; time:x[1];open:x[2]; high:x[3]; low:x[4]; close:x[5]; amount:x[6]; volume:x[7])}
szret:{[x] format flip concat (getszdatafz[x] peach)szstockindex}

shstockindex:til count shtosavestock
getshdatafz:{[y;x] readtdxfz[shtosavestock[x];shstocklist[x]; y]}
shret:{[x] format flip concat (getshdatafz[x] peach)shstockindex}
minutemain:{[x;y] (`$ path2,(string y),"/1m/") set .Q.en[`$ -1_path2] x}
iterdate1m:{[x] minutemain[szret[x] upsert shret[x];x]}

// =======================================================总控=======================================================
show tosavedate1m
show .z.Z
(iterdate1m') tosavedate1m  // 这里不要peach，不然会导致多个线程写sym冲突
show .z.Z