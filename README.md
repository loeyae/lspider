># Color-Data数据采集框架

>## 版本要求
>* python>=3.5

>## 安装方法
>* python3 setup.py sdist
>* pip3 install dist/cdspider-0.1.tar.gz

>## 卸载方法
>* pip3 uninstall cdspider

>## 运行方法
>* /usr/local/bin/cdspider [command]
>* /usr/local/bin/cdspider --help 获取command

>## supervisor 配置
> 以下配置的${rootpath}为具体安装路径，也可将配置文件整体换到单独的配置文件。配置文件中的数据库、队列等配置，需根据实际部署环境进行调整
> numprocs：可根据实际情况，增加进程个数。route只能单进程运行，否则会造成分发混乱
>
> ;route
> [program:cdspider_route]
> command=/usr/local/bin/cdspider -c ${rootpath}/config/main.server.json route
> process_name=%(program_name)s_%(process_num)02d
> numprocs=1

> ;newtask_schedule
> [program:cdspider_newtask_schedule]
> command=/usr/local/bin/cdspider -c ${rootpath}/config/main.server.json newtask-schedule
> process_name=%(program_name)s_%(process_num)02d
> numprocs=1

> ;plantask_schedule
> [program:cdspider_plantask_schedule]
> command=/usr/local/bin/cdspider -c ${rootpath}/config/main.server.json plantask-schedule
> process_name=%(program_name)s_%(process_num)02d
> numprocs=1

> ;synctask_schedule
> [program:cdspider_synctask_schedule]
> command=/usr/local/bin/cdspider -c ${rootpath}/config/main.server.json synctask-schedule
> process_name=%(program_name)s_%(process_num)02d
> numprocs=1

> ;status_schedule
> [program:cdspider_status_schedule]
> command=/usr/local/bin/cdspider -c ${rootpath}/config/main.server.json status-schedule
> process_name=%(program_name)s_%(process_num)02d
> numprocs=1

> ;search_work
> [program:cdspider_search_schedule]
> command=/usr/local/bin/cdspider -c ${rootpath}/config/main.server.json search-schedule
> process_name=%(program_name)s_%(process_num)02d
> numprocs=1

> ;fetch
> [program:cdspider_fetch]
> command=/usr/local/bin/cdspider -c ${rootpath}/config/main.server.json fetch
> process_name=%(program_name)s_%(process_num)02d
> numprocs=1

> ;spider_rpc
> [program:cdspider_spider_rpc]
> command=/usr/local/bin/cdspider -c ${rootpath}/config/main.server.json spider-rpc
> process_name=%(program_name)s_%(process_num)02d
> numprocs=1

> ;exc_work
> [program:cdspider_exc_work]
> command=/usr/local/bin/cdspider -c ${rootpath}/config/main.server.json exc-work
> process_name=%(program_name)s_%(process_num)02d
> numprocs=1
