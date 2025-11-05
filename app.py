from flask import Flask, request, jsonify
from flask_cors import CORS
import os

# 导入自定义模块 DatabaseConnector
from db_connector import DatabaseConnector  # 确保正确导入

app = Flask(__name__)
CORS(app)  # 允许跨域请求

# 移除所有静态文件服务相关的路由

@app.route('/api/tables/search', methods=['POST'])
def search_tables():
    """根据描述搜索表"""
    try:
        data = request.get_json()
        db_type = data.get('db_type', '').strip()
        search_text = data.get('search_text', '').strip()

        print(f"收到的请求数据：db_type = {db_type}, search_text = {search_text}")  # 调试输出

        if not db_type or not search_text:
            return jsonify({
                'success': False,
                'message': '数据库类型或搜索关键词不能为空',
                'data': []
            })

        db_connector = DatabaseConnector(db_type)

        results = db_connector.search_tables_by_description(search_text)
        print(f"查询结果：{results}")  # 调试输出

        return jsonify({
            'success': True,
            'message': f'找到 {len(results)} 个表',
            'data': results
        })
    except Exception as e:
        print(f"错误信息：{str(e)}")  # 打印详细错误
        return jsonify({
            'success': False,
            'message': f'搜索失败: {str(e)}',
            'data': []
        })

# 可选：添加健康检查接口
@app.route('/api/health')
def health_check():
    return jsonify({'status': 'healthy', 'service': 'table-search-api'})

if __name__ == '__main__':
    app.run(debug=True, host='0.0.0.0', port=5000)  # 指定端口便于前端调用
