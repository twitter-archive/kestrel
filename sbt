java -Xmx1024M -XX:MaxPermSize=512m -jar `dirname $0`/lib/sbt-launch.jar -Dsbt.override.build.repos=true -Dsbt.repository.config=project/repository "$@"
