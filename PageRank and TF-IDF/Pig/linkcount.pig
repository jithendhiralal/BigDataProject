crawl = load 'flat.txt' as (url:chararray, link:chararray);
id = foreach crawl generate url, flatten(link) as link;

byurl = group id by url;
bylink = group id by link;

outlink_count = foreach byurl generate group, COUNT(id.url) as ocount;

inlink_count = foreach bylink generate group, COUNT(id.link) as icount;

jnd = join outlink_count by group, inlink_count by group;
res = foreach jnd generate $0, $1, $3;
srt = order res by inlink_count::icount desc;
store srt into 'links';


