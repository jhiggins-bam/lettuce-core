package io.lettuce.core.push;

public interface RedisPushListener {

    void onPush(String type, Object args);

}
