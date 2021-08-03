# Makefile
# Data Culpa Python Client
#
# Copyright (c) 2020-2021 Data Culpa, Inc.
#
# Permission is hereby granted, free of charge, to any person obtaining a copy
# of this software and associated documentation files (the "Software"), to 
# deal in the Software without restriction, including without limitation the 
# rights to use, copy, modify, merge, publish, distribute, sublicense, and/or
# sell copies of the Software, and to permit persons to whom the Software is
# furnished to do so, subject to the following conditions:
#
# The above copyright notice and this permission notice shall be included in
# all copies or substantial portions of the Software.
#
# THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS 
# OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
# FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
# AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
# LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING 
# FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER 
# DEALINGS IN THE SOFTWARE.
#


.PHONY: pypi
pypi:
	$(MAKE) -C src/dataculpa
	$(MAKE) -C test
	@echo "**** Building python package..."
	python3 -m build
	@echo
	@echo "**** Use 'make upload' to push to PyPI"

.PHONY: clean
clean:
	@echo "**** Cleaning up..."
	$(RM) -rf dist
	$(RM) -rf build
	$(RM) -rf src/dataculpa/__pycache__


.PHONY: upload
upload:
	@echo "**** Uploading... you'll need the secret in your keyring or enter it when prompted."
	python3 -m twine upload dist/* --username __token__
