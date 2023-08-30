# 使用官方Ubuntu映像作為基礎
FROM ubuntu:18.04

RUN mkdir flask
COPY ./flask_api flask/flask_api

# 更新Ubuntu並安裝必要的套件
RUN apt-get update && apt-get install -y \
    vim\
    curl \
    wget \
    bzip2 \
    ca-certificates \
    libglib2.0-0 \
    libxext6 \
    libsm6 \
    libxrender1

# 安裝Miniconda
ENV CONDA_HOME /opt/conda
ENV PATH $CONDA_HOME/bin:$PATH

# -k 可以忽略ssl的驗證
RUN curl -k -fsSL https://repo.anaconda.com/miniconda/Miniconda3-latest-Linux-x86_64.sh -o miniconda.sh \ 
    && bash miniconda.sh -b -p $CONDA_HOME \
    && rm miniconda.sh \
    && conda clean -afy

# 設置Conda環境（選擇性）
RUN conda config --set ssl_verify false
RUN conda create -n flask python=3.7
SHELL ["conda", "run", "-n", "flask", "/bin/bash", "-c"]

RUN pip install --trusted-host pypi.org --trusted-host pypi.python.org --trusted-host files.pythonhosted.org flask-redis==0.4.0
RUN pip install --trusted-host pypi.org --trusted-host pypi.python.org --trusted-host files.pythonhosted.org pandas==1.1.5
RUN pip install --trusted-host pypi.org --trusted-host pypi.python.org --trusted-host files.pythonhosted.org numpy==1.19.5
RUN pip install --trusted-host pypi.org --trusted-host pypi.python.org --trusted-host files.pythonhosted.org pymssql

# 啟動bash shell作為入口點
CMD ["/bin/bash"]