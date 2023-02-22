--------------------------------------------------------
--  DDL for Table ACCOUNTS
--------------------------------------------------------

  CREATE TABLE "ACCOUNTS" 
   (	"ACCOUNT_NAME" VARCHAR2(128 BYTE) COLLATE "USING_NLS_COMP", 
	"ACCOUNT_INTEREST" VARCHAR2(128 BYTE) COLLATE "USING_NLS_COMP"
   );
  GRANT DELETE ON "ACCOUNTS" TO "PSADMIN";
  GRANT INSERT ON "ACCOUNTS" TO "PSADMIN";
  GRANT SELECT ON "ACCOUNTS" TO "PSADMIN";
  GRANT UPDATE ON "ACCOUNTS" TO "PSADMIN";
--------------------------------------------------------
--  DDL for Table CHAT
--------------------------------------------------------

  CREATE TABLE "CHAT" 
   (	"STREAMER" VARCHAR2(50 BYTE) COLLATE "USING_NLS_COMP", 
	"CHANNEL_ID" VARCHAR2(50 BYTE) COLLATE "USING_NLS_COMP", 
	"TIMESTAMP" VARCHAR2(50 BYTE) COLLATE "USING_NLS_COMP", 
	"FORMATTED_DATE" DATE, 
	"MESSAGE_ID" VARCHAR2(50 BYTE) COLLATE "USING_NLS_COMP", 
	"MESSAGE_TYPE" VARCHAR2(50 BYTE) COLLATE "USING_NLS_COMP", 
	"MESSAGE" VARCHAR2(1000 BYTE) COLLATE "USING_NLS_COMP", 
	"AUTHOR_ID" VARCHAR2(50 BYTE) COLLATE "USING_NLS_COMP", 
	"AUTHOR_NAME" VARCHAR2(50 BYTE) COLLATE "USING_NLS_COMP", 
	"AUTHOR_IS_MODERATOR" NUMBER, 
	"AUTHOR_IS_SUBSCRIBER" NUMBER, 
	"AUTHOR_IS_TURBO" NUMBER, 
	"SENTIMENT" VARCHAR2(50 BYTE) COLLATE "USING_NLS_COMP" DEFAULT '-1', 
	"HOW_POSITIVE" VARCHAR2(50 BYTE) COLLATE "USING_NLS_COMP" DEFAULT '-1', 
	"HOW_NEUTRAL" VARCHAR2(50 BYTE) COLLATE "USING_NLS_COMP" DEFAULT '-1', 
	"HOW_NEGATIVE" VARCHAR2(50 BYTE) COLLATE "USING_NLS_COMP" DEFAULT '-1', 
	"LANGUAGE" VARCHAR2(20 BYTE) COLLATE "USING_NLS_COMP" DEFAULT 'UNKNOWN', 
	"ORIGINAL_MESSAGE" VARCHAR2(1000 BYTE) COLLATE "USING_NLS_COMP", 
	"TRANSLATED" VARCHAR2(1 BYTE) COLLATE "USING_NLS_COMP" DEFAULT '0', 
	"ORIGINAL_LANGUAGE" VARCHAR2(4000 BYTE) COLLATE "USING_NLS_COMP"
   );
--------------------------------------------------------
--  DDL for Table CHAT_USERS
--------------------------------------------------------

  CREATE TABLE "CHAT_USERS" 
   (	"AUTHOR_NAME" VARCHAR2(50 BYTE) COLLATE "USING_NLS_COMP", 
	"OSINT_PROCESSED" VARCHAR2(1 BYTE) COLLATE "USING_NLS_COMP"
   );
--------------------------------------------------------
--  DDL for Table OSINT
--------------------------------------------------------

  CREATE TABLE "OSINT" 
   (	"TWITCH_USERNAME" VARCHAR2(128 BYTE) COLLATE "USING_NLS_COMP", 
	"SITE_FOUND" VARCHAR2(256 BYTE) COLLATE "USING_NLS_COMP"
   ) ;
--------------------------------------------------------
--  DDL for Table VOD_CHAT
--------------------------------------------------------

  CREATE TABLE "VOD_CHAT" 
   (	"VOD_URL" VARCHAR2(128 BYTE) COLLATE "USING_NLS_COMP", 
	"TIMESTAMP" VARCHAR2(50 BYTE) COLLATE "USING_NLS_COMP", 
	"FORMATTED_DATE" DATE, 
	"MESSAGE_TYPE" VARCHAR2(50 BYTE) COLLATE "USING_NLS_COMP", 
	"MESSAGE" VARCHAR2(1000 BYTE) COLLATE "USING_NLS_COMP", 
	"AUTHOR_ID" VARCHAR2(100 BYTE) COLLATE "USING_NLS_COMP", 
	"AUTHOR_NAME" VARCHAR2(50 BYTE) COLLATE "USING_NLS_COMP", 
	"SENTIMENT" VARCHAR2(50 BYTE) COLLATE "USING_NLS_COMP" DEFAULT '-1', 
	"HOW_POSITIVE" VARCHAR2(50 BYTE) COLLATE "USING_NLS_COMP" DEFAULT '-1', 
	"HOW_NEUTRAL" VARCHAR2(50 BYTE) COLLATE "USING_NLS_COMP" DEFAULT '-1', 
	"HOW_NEGATIVE" VARCHAR2(50 BYTE) COLLATE "USING_NLS_COMP" DEFAULT '-1', 
	"MESSAGE_ID" VARCHAR2(128 BYTE) COLLATE "USING_NLS_COMP", 
	"LANGUAGE" VARCHAR2(20 BYTE) COLLATE "USING_NLS_COMP" DEFAULT 'UNKNOWN', 
	"ORIGINAL_MESSAGE" VARCHAR2(1000 BYTE) COLLATE "USING_NLS_COMP", 
	"TRANSLATED" VARCHAR2(1 BYTE) COLLATE "USING_NLS_COMP" DEFAULT '0', 
	"ORIGINAL_LANGUAGE" VARCHAR2(4000 BYTE) COLLATE "USING_NLS_COMP"
   );
--------------------------------------------------------
--  DDL for Table VODS
--------------------------------------------------------

  CREATE TABLE "VODS" 
   (	"STREAMER_NAME" VARCHAR2(128 BYTE) COLLATE "USING_NLS_COMP", 
	"VOD_URL" VARCHAR2(100 BYTE) COLLATE "USING_NLS_COMP", 
	"PROCESSED" VARCHAR2(1 BYTE) COLLATE "USING_NLS_COMP"
   );
--------------------------------------------------------
--  DDL for Index ACCOUNTS_PK
--------------------------------------------------------

  CREATE UNIQUE INDEX "ACCOUNTS_PK" ON "ACCOUNTS" ("ACCOUNT_NAME") ;
--------------------------------------------------------
--  DDL for Index CHAT_PK
--------------------------------------------------------

  CREATE UNIQUE INDEX "CHAT_PK" ON "CHAT" ("STREAMER", "MESSAGE_ID") ;
--------------------------------------------------------
--  DDL for Index CHAT_USERS_PK
--------------------------------------------------------

  CREATE UNIQUE INDEX "CHAT_USERS_PK" ON "CHAT_USERS" ("AUTHOR_NAME");
--------------------------------------------------------
--  DDL for Index OSINT_PK
--------------------------------------------------------

  CREATE UNIQUE INDEX "OSINT_PK" ON "OSINT" ("TWITCH_USERNAME", "SITE_FOUND");
--------------------------------------------------------
--  DDL for Index VOD_CHAT_PK
--------------------------------------------------------

  CREATE UNIQUE INDEX "VOD_CHAT_PK" ON "VOD_CHAT" ("VOD_URL", "MESSAGE_ID") ;
--------------------------------------------------------
--  DDL for Index VODS_PK
--------------------------------------------------------

  CREATE UNIQUE INDEX "VODS_PK" ON "VODS" ("VOD_URL");
--------------------------------------------------------
--  Constraints for Table ACCOUNTS
--------------------------------------------------------

  ALTER TABLE "ACCOUNTS" MODIFY ("ACCOUNT_NAME" NOT NULL ENABLE);
--------------------------------------------------------
--  Constraints for Table CHAT
--------------------------------------------------------

  ALTER TABLE "CHAT" MODIFY ("CHANNEL_ID" NOT NULL ENABLE);
  ALTER TABLE "CHAT" MODIFY ("TIMESTAMP" NOT NULL ENABLE);
  ALTER TABLE "CHAT" MODIFY ("FORMATTED_DATE" NOT NULL ENABLE);
  ALTER TABLE "CHAT" MODIFY ("MESSAGE_TYPE" NOT NULL ENABLE);
  ALTER TABLE "CHAT" MODIFY ("MESSAGE" NOT NULL ENABLE);
  ALTER TABLE "CHAT" MODIFY ("AUTHOR_ID" NOT NULL ENABLE);
  ALTER TABLE "CHAT" MODIFY ("AUTHOR_NAME" NOT NULL ENABLE);
  ALTER TABLE "CHAT" MODIFY ("AUTHOR_IS_MODERATOR" NOT NULL ENABLE);
  ALTER TABLE "CHAT" MODIFY ("AUTHOR_IS_SUBSCRIBER" NOT NULL ENABLE);
  ALTER TABLE "CHAT" MODIFY ("AUTHOR_IS_TURBO" NOT NULL ENABLE);
  ALTER TABLE "CHAT" ADD CONSTRAINT "CHAT_PK" PRIMARY KEY ("STREAMER", "MESSAGE_ID");
--------------------------------------------------------
--  Constraints for Table CHAT_USERS
--------------------------------------------------------

  ALTER TABLE "CHAT_USERS" MODIFY ("AUTHOR_NAME" NOT NULL ENABLE);
  ALTER TABLE "CHAT_USERS" ADD CONSTRAINT "CHAT_USERS_PK" PRIMARY KEY ("AUTHOR_NAME");
--------------------------------------------------------
--  Constraints for Table OSINT
--------------------------------------------------------

  ALTER TABLE "OSINT" ADD CONSTRAINT "OSINT_PK" PRIMARY KEY ("TWITCH_USERNAME", "SITE_FOUND");
--------------------------------------------------------
--  Constraints for Table VOD_CHAT
--------------------------------------------------------

  ALTER TABLE "VOD_CHAT" MODIFY ("VOD_URL" NOT NULL ENABLE);
  ALTER TABLE "VOD_CHAT" MODIFY ("TIMESTAMP" NOT NULL ENABLE);
  ALTER TABLE "VOD_CHAT" MODIFY ("MESSAGE" NOT NULL ENABLE);
  ALTER TABLE "VOD_CHAT" MODIFY ("AUTHOR_ID" NOT NULL ENABLE);
  ALTER TABLE "VOD_CHAT" ADD CONSTRAINT "VOD_CHAT_PK" PRIMARY KEY ("VOD_URL", "MESSAGE_ID");
--------------------------------------------------------
--  Constraints for Table VODS
--------------------------------------------------------

  ALTER TABLE "VODS" MODIFY ("STREAMER_NAME" NOT NULL ENABLE);
  ALTER TABLE "VODS" ADD CONSTRAINT "VODS_PK" PRIMARY KEY ("VOD_URL");
