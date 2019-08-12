from lanfang import network
import unittest
import logging


class TestSmtpClient(unittest.TestCase):
  def setUp(self):
    logging.basicConfig(level=logging.INFO)
    self._m_client = network.email.SMTPClient(
      'name', 'user@domain.com', 'password')

  def tearDown(self):
    pass

  def test_send(self):
    content = """<html>
    <h1>测试邮件</h1>
    <p>算法工程师</p>
    <p><img src="cid:1"></p>
    </html>
    """
    self._m_client.send(
      [("张三", "zhangsan@qq.com"), ("李四", "lisi@qq.com")],
      "深度学习为什么会出现梯度爆炸",
      content,
      [__file__],
      content_type="html")


if __name__ == "__main__":
  unittest.main()
