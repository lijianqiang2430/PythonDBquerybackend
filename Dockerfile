# 使用python的版本
FROM python:3.8

# 设置工作目录
WORKDIR /app

# 讲应用文件复制到工作目录
COPY . /app

# 安装依赖程序
RUN pip install -r requirements.txt

# 定义入口
CMD ["python", "app.py"]
