># Color-Data数据采集框架

>## 版本要求
>* python>=3.5

>## 安装方法
>* python setup.py sdist
>* pip install dist/cdspider-0.1.tar.gz

>## 卸载方法
>* pip uninstall cdspider

>## 运行方法
>* cdspider [command]
>* cdspider --help 获取command

>## apache wsgi部署
>* <VirtualHost *:80>
>*     DocumentRoot "{rootdir}"
>*     ServerName {ServerName}
>*     ServerAlias {ServerName}
>*
>*     WSGIScriptAlias / "{rootdir}/wsgi.py"
>*
>*     <Directory "{rootdir}">
>*         Options Indexes FollowSymLinks MultiViews
>*         AllowOverride all
>*         <IfDefine APACHE24>
>*             Require local
>*         </IfDefine>
>*         <IfDefine !APACHE24>
>*             Order Deny,Allow
>*             Deny from all
>*             Allow from localhost ::1 127.0.0.1
>*         </IfDefine>
>*         WSGIScriptReloading On
>*     </Directory>
>* </VirtualHost>
