SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO

CREATE TABLE [dbo].[tblJaroWinkler_Universe](
	[ID] [numeric](38, 0) IDENTITY(1,1) NOT NULL,
	[PatientID_A] [int] NOT NULL,
	[PatientID_B] [int] NOT NULL,
	[FirstName_A] [varchar](256) NULL,
	[LastName_A] [varchar](256) NULL,
	[FirstName_B] [varchar](256) NULL,
	[LastName_B] [varchar](256) NULL,
	[DOB_A] [varchar](256) NULL,
	[DOB_B] [varchar](256) NULL,
	[Zip_A] [varchar](256) NULL,
	[Zip_B] [varchar](256) NULL,
	[Address1_A] [varchar](256) NULL,
	[Address1_B] [varchar](256) NULL,
	[PrimaryPhone_A] [varchar](256) NULL,
	[PrimaryPhone_B] [varchar](256) NULL,
	[SecondaryPhone_A] [varchar](256) NULL,
	[SecondaryPhone_B] [varchar](256) NULL,
	[Email_A] [varchar](256) NULL,
	[Email_B] [varchar](256) NULL,
	[FirstName_LastName_DOB] [float] NULL,
	[FirstName_LastName_DOB_Zip3] [float] NULL,
	[FirstName_LastName_Address_Zip3] [float] NULL,
	[PrimaryPhone_Address_Zip] [float] NULL,
	[Address_Zip_Email] [float] NULL,
	[JW_Batch_ID] [numeric](38, 0) NULL,
	[DateCreated] [datetime] NOT NULL,
	[DateModified] [datetime] NOT NULL,
	[LastModifiedBy] [varchar](50) NULL,
	[MergedResult] [varchar](50) NULL,
 CONSTRAINT [PK_tblJaroWinkler_Universe] PRIMARY KEY CLUSTERED
(
	[ID] ASC
)WITH (PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, IGNORE_DUP_KEY = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON, FILLFACTOR = 90, DATA_COMPRESSION = PAGE) ON [IX1] --[PRIMARY]
) ON [PRIMARY]
GO

ALTER TABLE [dbo].[tblJaroWinkler_Universe] ADD  CONSTRAINT [DF_tblJaroWinkler_Universe_DateCreated]  DEFAULT (getdate()) FOR [DateCreated]
GO

ALTER TABLE [dbo].[tblJaroWinkler_Universe] ADD  CONSTRAINT [DF_tblJaroWinkler_Universe_DateModified]  DEFAULT (getdate()) FOR [DateModified]
GO