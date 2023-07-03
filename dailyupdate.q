// ==================================daily===============================
path2:":D:/faca/db/"
func1: {[x] type key `$ path2,(string x)}
filelist: key `$ path2
findall:{where x~\:y}
folderindex: findall[(func1')filelist; 11h]
folderlist:filelist[folderindex]
exist:{[x] not()~key x}
func2:{[x]exist `$ path2,x,"/1d"}
dbdatelist: folderlist[where (func2') string folderlist]

pathsz:":D:/work/tdx/vipdoc/sz/lday/"
pathsh:":D:/work/tdx/vipdoc/sh/lday/"
stock:"sh000001.day"
readtdx:{[x] ("iiiiieii";4 4 4 4 4 4 4 4) 1: x}
ret: readtdx `$ pathsh,stock
tdxdatelist:`$ string ret[0]
droplist:{[x;y] x _ x?y}
tosavedate1d:droplist/[tdxdatelist;dbdatelist]

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

sec_type: `sz00`sz30`sh60`sh68`sz39`sh00`sh88`sh99!(0.01 0.01;0.01 0.01;0.01 0.01;0.01 0.01;0.01 1.0;0.01 1.0;0.01 1.0;0.01 1.0)
getvar:{[x] sec_type[`$ 0 4 sublist string x]}
filllist:{[x;y] x}
stockcolumn: {[x;y] (filllist[x] peach)[til count y]}
makedtkey: {[x;y] (`$ string x[0])!flip(x[1]*getvar[y][0];x[2]*getvar[y][0];x[3]*getvar[y][0];x[4]*getvar[y][0];x[5]*getvar[y][0];x[6]*getvar[y][1]; stockcolumn[y; x[0]])}
handleinvalid:{[x;y;z] $[y in key x; x[y]; `0`0`0`0`0`0 upsert z]}
readtdx1d:{[x;y;z] handleinvalid[makedtkey[("iiiiieii";4 4 4 4 4 4 4 4) 1: x;y];z; y]}  / x:file y:code z:date

szstockindex:til count sztosavestock
dropna:{[x] x[where 1-'x[;0]~\:`0]}
getszdata:{[y;x] readtdx1d[sztosavestock[x];szstocklist[x]; y]}
format:{[x] ([] sym:x[6]; open:x[0]; high:x[1]; low:x[2]; close:x[3]; amount:x[4]; volume:x[5])}
szret:{[x] format flip dropna(getszdata[x] peach)szstockindex}
shstockindex:til count shtosavestock
getshdata:{[y;x] readtdx1d[shtosavestock[x];shstocklist[x]; y]} // y:date x:stockindex
shret:{[x] format flip dropna(getshdata[x] peach)shstockindex}

dailymain:{[x;y](`$ path2,(string y),"/1d/") set .Q.en[`$ -1_path2] x}
iterdate1d:{[x] dailymain[szret[x] upsert shret[x];x]}


// =======================================================总控=======================================================
show tosavedate1d
show .z.Z
(iterdate1d') tosavedate1d  // 这里不要peach，不然会导致多个线程写sym冲突
//(iterdate1m') tosavedate1m
show .z.Z

