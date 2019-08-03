package com.tony.hbase.model;

import lombok.Data;
import lombok.experimental.Accessors;

import java.math.BigDecimal;
import java.util.Date;

/**
 * @author tony
 * @describe TransactionDTO
 * @date 2019-08-02
 */
@Data
@Accessors(chain = true)
public class TransactionDTO {
    private static final long serialVersionUID = 1L;
    private String seq;
    private Integer channelId;
    private int transactionCode;
    private String transactionId;
    private String certificateType;
    private String certificateNumber;
    private String accountNumber;
    private String accountType;
    private String phoneNumber;
    private Date transactionTime;
    private BigDecimal transactionAmount;
    private String transactionType;
    private String orderNumber;
    private String receiveStr;
    private String virtualProduct;
    private String merchantNumber;
    private Integer dealTime;
    private Integer programHandleType;
    private Integer isRisk;
    private Integer riskLevel;
    private Integer riskConfirmType;
    private Date riskHandleTime;
    private String riskHandleUserId;
    private String callNumber;
    private String remark;
    private Date createTime;
    private Date updateTime;
    private Integer strategySeq;
}

