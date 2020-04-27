# 文件格式

## DataFile

初始文件大小：16K
页大小：4K
一个文件中一开始有4页

### Page

第1个字节: 当前文件中存储记录总数n
第2-8个字节：前面用于记录每个record是否有效，若有效则为1，无效为0；后面空着
第9-？个字节：存储记录

## MetaFile

第1-2个字节：记录一个记录占据的字节数
第3-4个字节：记录空白链表的长度
后面每2个字节记录一个有空行的page的id, id从0开始
