# lanfang

**lanfang** is a toolkit to make daily work and learning more efficient.







#### TODO

1. Runner: Generate sharing parameters automatically through command and funcation call.



## Related Materials

### Python Packaging Tools

#### Reference

- [Packaging Python Projects](https://packaging.python.org/tutorials/packaging-projects/)

#### Method

```shell
pip install --user --upgrade setuptools wheel
python setup.py sdist bdist_wheel

pip install --upgrade twine
# test server
twine upload --repository-url https://test.pypi.org/legacy/ dist/*
# production server
twine upload dist/*
```