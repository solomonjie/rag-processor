操作	命令 (在 PowerShell 或 CMD 中运行)	说明
1. 创建虚拟环境	python -m venv option_env	在当前目录下创建一个名为 my_project_env 的虚拟环境文件夹。
2. 激活虚拟环境	option_env\Scripts\activate	(可选：用于 CMD 或 Git Bash) 激活环境。
3. 退出虚拟环境	deactivate	退出当前的虚拟环境，回到系统环境。
4. 删除虚拟环境	rmdir /s /q option_env	(可选：用于 CMD) 递归删除虚拟环境文件夹。