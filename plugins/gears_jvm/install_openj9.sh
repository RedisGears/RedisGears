SRCDIR=../../deps/openj9-openjdk-jdk14

FREEMARKER_JAR_PATH=$(realpath ./freemarker.jar)
echo $FREEMARKER_JAR_PATH

JDK_PATH=$(realpath ./jdk-14.0.1+7/)
echo $JDK_PATH

if [ ! -f "$FREEMARKER_JAR_PATH" ]; then
	wget https://sourceforge.net/projects/freemarker/files/freemarker/2.3.8/freemarker-2.3.8.tar.gz/download -O freemarker.tgz;\
	tar -xzf freemarker.tgz freemarker-2.3.8/lib/freemarker.jar --strip=2;\
	rm -f freemarker.tgz;\
fi

if [ ! -d "$JDK_PATH" ]; then
	wget -O bootjdk14.tar.gz "https://api.adoptopenjdk.net/v3/binary/latest/14/ga/linux/x64/jdk/openj9/normal/adoptopenjdk";\
	tar -xzf bootjdk14.tar.gz;\
	rm -f bootjdk14.tar.gz
fi

cd $SRCDIR; bash get_source.sh

cd $SRCDIR; bash configure --with-freemarker-jar=$FREEMARKER_JAR_PATH --with-boot-jdk=$JDK_PATH

make all -C $SRCDIR