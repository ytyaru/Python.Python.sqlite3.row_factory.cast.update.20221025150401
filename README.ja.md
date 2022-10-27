[en](./README.md)

# Python.sqlite3.row_factory.cast.update

　update文をラップする。

<!--

# デモ

* [demo](https://ytyaru.github.io/Python.sqlite3.row_factory.cast.update.20221025150401/)

![img](https://github.com/ytyaru/Python.sqlite3.row_factory.cast.update.20221025150401/blob/master/doc/0.png?raw=true)

# 特徴

* セールスポイント

-->

# 開発環境

* <time datetime="2022-10-25T15:03:59+0900">2022-10-25</time>
* [Raspbierry Pi](https://ja.wikipedia.org/wiki/Raspberry_Pi) 4 Model B Rev 1.2
* [Raspberry Pi OS](https://ja.wikipedia.org/wiki/Raspbian) buster 10.0 2020-08-20 <small>[setup](http://ytyaru.hatenablog.com/entry/2020/10/06/111111)</small>
* bash 5.0.3(1)-release
* Python 3.10.5

<!-- * Python 2.7.16 -->

```sh
$ uname -a
Linux raspberrypi 5.10.103-v7l+ #1529 SMP Tue Mar 8 12:24:00 GMT 2022 armv7l GNU/Linux
```

# インストール

## anyenv

```sh
git clone https://github.com/anyenv/anyenv ~/.anyenv
echo 'export PATH="$HOME/.anyenv/bin:$PATH"' >> ~/.bash_profile
echo 'eval "$(anyenv init -)"' >> ~/.bash_profile
anyenv install --init -y
```

## pyenv

```sh
anyenv install pyenv
exec $SHELL -l
```

## python

```sh
sudo apt install -y libsqlite3-dev libbz2-dev libncurses5-dev libgdbm-dev liblzma-dev libssl-dev tcl-dev tk-dev libreadline-dev
```

```sh
pyenv install -l
```
```sh
pyenv install 3.10.5
```


## このリポジトリ

```sh
git clone https://github.com/ytyaru/Python.sqlite3.row_factory.cast.update.20221025150401
cd Python.sqlite3.row_factory.cast.update.20221025150401/src
```

# 使い方

## 実行

```sh
./run.py
```

## 単体テスト

```sh
./test.py
```

<!--

# 注意

* 注意点など

-->

# 著者

　ytyaru

* [![github](http://www.google.com/s2/favicons?domain=github.com)](https://github.com/ytyaru "github")
* [![hatena](http://www.google.com/s2/favicons?domain=www.hatena.ne.jp)](http://ytyaru.hatenablog.com/ytyaru "hatena")
* [![twitter](http://www.google.com/s2/favicons?domain=twitter.com)](https://twitter.com/ytyaru1 "twitter")
* [![mastodon](http://www.google.com/s2/favicons?domain=mstdn.jp)](https://mstdn.jp/web/accounts/233143 "mastdon")

# ライセンス

　このソフトウェアはCC0ライセンスである。

[![CC0](http://i.creativecommons.org/p/zero/1.0/88x31.png "CC0")](http://creativecommons.org/publicdomain/zero/1.0/deed.ja)

