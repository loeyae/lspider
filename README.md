># 数据采集框架

>## 版本要求
>* python>=3.5

>## 安装方法
>* cd ${rootpath}
>* python3 setup.py sdist
>* pip3 install dist/cdspider-0.1.tar.gz

>## 卸载方法
>* pip3 uninstall cdspider

>## 运行方法
>直接运行cdspider
>* /usr/local/bin/cdspider [options] [command] [options]
>* /usr/local/bin/cdspider --help                      获取帮助
>* /usr/local/bin/cdspider [command] --help            获取帮助
>
>通过python module方式
>* python3 -m cdspider.run [options] [command] [options]
>* python3 -m cdspider.run --help                      获取帮助
>* python3 -m cdspider.run [command] --help            获取帮助
>
>通过run.py, cd到相应目录
>* python3 run.py [options] [command] [options]
>* python3 run.py --help                    获取帮助
>* python3 run.py [command] --help          获取帮助

>## supervisor 配置
> 以下配置的${rootpath}为具体安装路径，也可将配置文件整体换到单独的配置文件。配置文件中的数据库、队列等配置，需根据实际部署环境进行调整
> numprocs：可根据实际情况，增加进程个数。route只能单进程运行，否则会造成分发混乱
> /usr/local/bin/cdspider命令的位置会根据python的安装方式而不同，有可能存在于python自身的bin目录，window系统则是在Scripts目录
>
> ;route
> [program:cdspider_route]
> command=/usr/local/bin/cdspider -c config/main.server.json route
> process_name=%(program_name)s_%(process_num)02d
> numprocs=1
> directory=${rootpath}
> umask=022
> priority=999
> autostart=true
> autorestart=true

> ;schedule_rpc
> [program:cdspider_schedule]
> command=/usr/local/bin/cdspider -c config/main.server.json schedule-rpc
> process_name=%(program_name)s_%(process_num)02d
> numprocs=1
> directory=${rootpath}
> umask=022
> priority=999
> autostart=true
> autorestart=true


> ;schedule
> [program:cdspider_schedule]
> command=/usr/local/bin/cdspider -c config/main.server.json schedule
> process_name=%(program_name)s_%(process_num)02d
> numprocs=1
> directory=${rootpath}
> umask=022
> priority=999
> autostart=true
> autorestart=true

> ;fetch
> [program:cdspider_fetch]
> command=/usr/local/bin/cdspider -c config/main.server.json fetch
> process_name=%(program_name)s_%(process_num)02d
> numprocs=1
> directory=${rootpath}
> umask=022
> priority=999
> autostart=true
> autorestart=true

> ;spider_rpc
> [program:cdspider_spider_rpc]
> command=/usr/local/bin/cdspider -c config/main.server.json spider-rpc
> process_name=%(program_name)s_%(process_num)02d
> numprocs=1
> directory=${rootpath}
> umask=022
> priority=999
> autostart=true
> autorestart=true

> ;work
> [program:cdspider_work_test]
> command=/usr/local/bin/cdspider -c config/main.server.json work --worker-cls cdspider.worker.TestWorker
> process_name=%(program_name)s_%(process_num)02d
> numprocs=1
> directory=${rootpath}
> umask=022
> priority=999
> autostart=true
> autorestart=true

> ;tool
> [program:cdspider_work_test]
> command=/usr/local/bin/cdspider -c config/main.server.json tool --tool-cls cdspider.tool.test_tool.test_tool
> process_name=%(program_name)s_%(process_num)02d
> numprocs=1
> directory=${rootpath}
> umask=022
> priority=999
> autostart=true
> autorestart=true