# filesync
文件同步

用法 

server ip端口

client ip端口  只比较文件大小进行同步

client ip端口  -crc32 额外使用crc32进行校验和对比

client ip端口  -md5 额外使用md5进行校验和对比，支持avx2和avx512的处理器可获得md5性能提升
