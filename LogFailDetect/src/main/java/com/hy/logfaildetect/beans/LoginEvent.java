package com.hy.logfaildetect.beans;

public class LoginEvent {
    private Long userId;
    private String ip;
    private String loginState;
    private Long timeStamp;

    public LoginEvent() {
    }

    public LoginEvent(Long userId, String ip, String loginState, Long timeStamp) {
        this.userId = userId;
        this.ip = ip;
        this.loginState = loginState;
        this.timeStamp = timeStamp;
    }

    public Long getUserId() {
        return userId;
    }

    public void setUserId(Long userId) {
        this.userId = userId;
    }

    public String getIp() {
        return ip;
    }

    public void setIp(String ip) {
        this.ip = ip;
    }

    public String getLoginState() {
        return loginState;
    }

    public void setLoginState(String loginState) {
        this.loginState = loginState;
    }

    public Long getTimeStamp() {
        return timeStamp;
    }

    public void setTimeStamp(Long timeStamp) {
        this.timeStamp = timeStamp;
    }

    @Override
    public String toString() {
        return "LoginEvent{" +
                "userId=" + userId +
                ", ip='" + ip + '\'' +
                ", loginState='" + loginState + '\'' +
                ", timeStamp=" + timeStamp +
                '}';
    }
}
