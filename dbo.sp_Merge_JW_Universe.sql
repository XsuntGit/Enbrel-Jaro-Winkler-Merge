USE [Enbrel_TestCRX]
GO
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
CREATE OR ALTER PROCEDURE [dbo].[sp_Merge_JW_Universe]
AS
SET NOCOUNT ON;
BEGIN TRY

	DECLARE @jw_threshold FLOAT = .9,
		@JW_Batch_ID NUMERIC(38,0)
	SELECT @JW_Batch_ID = NEXT VALUE FOR [dbo].[JW_Batch_Seq]

	MERGE [dbo].[tblJaroWinkler_Universe] as [target]
	USING (
		SELECT 	[PatientID_A],
			[PatientID_B],
			[FirstName_A],
			[LastName_A],
			[FirstName_B],
			[LastName_B],
			[DOB_A],
			[DOB_B],
			[Zip_A],
			[Zip_B],
			[Address1_A],
			[Address1_B],
			[PrimaryPhone_A],
			[PrimaryPhone_B],
			[SecondaryPhone_A],
			[SecondaryPhone_B],
			[Email_A],
			[Email_B],
			[FirstName_LastName_DOB],
			[FirstName_LastName_DOB_Zip3],
			[FirstName_LastName_Address_Zip3],
			[PrimaryPhone_Address_Zip],
			[Address_Zip_Email]
		FROM
		(
		SELECT DENSE_RANK() OVER(PARTITION BY Ranking_By_IDs ORDER BY [PatientID_A] ASC) as Partitioning_By_IDs, *
		FROM (
			SELECT DENSE_RANK() OVER(ORDER BY cast(tblA.PatientID as bigint)*cast(tblB.PatientID as bigint)) as Ranking_By_IDs,
				tblA.PatientID as PatientID_A,
				tblB.PatientID as PatientID_B,
				tblA.[FirstName] as FirstName_A,
				tblA.[LastName] as LastName_A,
				tblB.[FirstName] as FirstName_B,
				tblB.[LastName] as LastName_B,
				tblA.[DOB] as DOB_A,
				tblB.[DOB] as DOB_B,
				tblA.[Zip] as Zip_A,
				tblB.[Zip] as Zip_B,
				tblA.[Address1] as Address1_A,
				tblB.[Address1] as Address1_B,
				tblA.[PrimaryPhone] as PrimaryPhone_A,
				tblB.[PrimaryPhone] as PrimaryPhone_B,
				tblA.[SecondaryPhone] as SecondaryPhone_A,
				tblB.[SecondaryPhone] as SecondaryPhone_B,
				tblA.[Email] as Email_A,
				tblB.[Email] as Email_B,
				[dbo].[fn_CLR_Fuzzy_JaroWinkler](ISNULL(tblA.[FirstName],'') + ISNULL(tblA.[LastName],'') + [dbo].[fn_StripPatternFromString](tblA.[DOB],'[A-Za-z0-9]'), ISNULL(tblB.[FirstName],'') + ISNULL(tblB.[LastName],'') + [dbo].[fn_StripPatternFromString](tblB.[DOB],'[A-Za-z0-9]')) as FirstName_LastName_DOB,
				[dbo].[fn_CLR_Fuzzy_JaroWinkler](ISNULL(tblA.[FirstName],'') + ISNULL(tblA.[LastName],'') + [dbo].[fn_StripPatternFromString](tblA.[DOB],'[A-Za-z0-9]') + [dbo].[fn_StripPatternFromString](LEFT(tblA.[Zip],3),'[A-Za-z0-9]'), ISNULL(tblB.[FirstName],'') + ISNULL(tblB.[LastName],'') + [dbo].[fn_StripPatternFromString](tblB.[DOB],'[A-Za-z0-9]') + [dbo].[fn_StripPatternFromString](LEFT(tblB.[Zip],3),'[A-Za-z0-9]')) as FirstName_LastName_DOB_Zip3,
				[dbo].[fn_CLR_Fuzzy_JaroWinkler](ISNULL(tblA.[FirstName],'') + ISNULL(tblA.[LastName],'') + [dbo].[fn_StripPatternFromString](tblA.[Address1],'[A-Za-z0-9]') + [dbo].[fn_StripPatternFromString](LEFT(tblA.[Zip],3),'[A-Za-z0-9]'), ISNULL(tblA.[FirstName],'') + ISNULL(tblA.[LastName],'') + [dbo].[fn_StripPatternFromString](tblB.[Address1],'[A-Za-z0-9]') + [dbo].[fn_StripPatternFromString](LEFT(tblB.[Zip],3),'[A-Za-z0-9]')) as FirstName_LastName_Address_Zip3,
				[dbo].[fn_CLR_Fuzzy_JaroWinkler]([dbo].[fn_StripPatternFromString](tblA.[PrimaryPhone],'[A-Za-z0-9]') + [dbo].[fn_StripPatternFromString](tblA.[Address1],'[A-Za-z0-9]') + [dbo].[fn_StripPatternFromString](tblA.[Zip],'[A-Za-z0-9]'), [dbo].[fn_StripPatternFromString](tblB.[PrimaryPhone],'[A-Za-z0-9]') + [dbo].[fn_StripPatternFromString](tblB.[Address1],'[A-Za-z0-9]') + [dbo].[fn_StripPatternFromString](tblB.[Zip],'[A-Za-z0-9]')) as PrimaryPhone_Address_Zip,
				[dbo].[fn_CLR_Fuzzy_JaroWinkler]([dbo].[fn_StripPatternFromString](tblA.[Address1],'[A-Za-z0-9]') + [dbo].[fn_StripPatternFromString](tblA.[Zip],'[A-Za-z0-9]') + [dbo].[fn_StripPatternFromString](tblA.[Email],'[A-Za-z0-9]'), [dbo].[fn_StripPatternFromString](tblB.[Address1],'[A-Za-z0-9]') + [dbo].[fn_StripPatternFromString](tblB.[Zip],'[A-Za-z0-9]') + [dbo].[fn_StripPatternFromString](tblB.[Email],'[A-Za-z0-9]')) as Address_Zip_Email
			FROM [dbo].[tblPatientInfo] tblA
				CROSS JOIN
				(
					SELECT PatientID
						  ,[FirstName]
						  ,[LastName]
						  ,[Address1]
						  ,[Zip]
						  ,[PrimaryPhone]
						  ,[SecondaryPhone]
						  ,[Email]
						  ,[DOB]
					FROM [dbo].[tblPatientInfo] with (nolock)
  					WHERE [ActiveFlag] = 1
						AND [NeedToRunJaroWinkler] = 1
				) tblB
			WHERE tblA.[ActiveFlag] = 1
				AND tblA.[NeedToRunJaroWinkler] = 0
				AND [dbo].[fn_CLR_Fuzzy_JaroWinkler](ISNULL(tblA.[FirstName],'') + ISNULL(tblA.[LastName],'') + [dbo].[fn_StripPatternFromString](tblA.[DOB],'[A-Za-z0-9]') + [dbo].[fn_StripPatternFromString](LEFT(tblA.[Zip],3),'[A-Za-z0-9]'), ISNULL(tblB.[FirstName],'') + ISNULL(tblB.[LastName],'') + [dbo].[fn_StripPatternFromString](tblB.[DOB],'[A-Za-z0-9]') + [dbo].[fn_StripPatternFromString](LEFT(tblB.[Zip],3),'[A-Za-z0-9]')) >= @jw_threshold
		) tmp1
		) tmp2
		WHERE Partitioning_By_IDs = 1
	) as [source]
	ON ([target].[PatientID_A] = [source].PatientID_A AND [target].[PatientID_B] = [source].PatientID_B)
	WHEN MATCHED THEN
	UPDATE SET
	   [target].[FirstName_A] = [source].[FirstName_A],
	   [target].[LastName_A] = [source].[LastName_A],
	   [target].[FirstName_B] = [source].[FirstName_B],
	   [target].[LastName_B] = [source].[LastName_B],
	   [target].[DOB_A] = [source].[DOB_A],
	   [target].[DOB_B] = [source].[DOB_B],
	   [target].[Zip_A] = [source].[Zip_A],
	   [target].[Zip_B] = [source].[Zip_B],
	   [target].[Address1_A] = [source].[Address1_A],
	   [target].[Address1_B] = [source].[Address1_B],
	   [target].[PrimaryPhone_A] = [source].[PrimaryPhone_A],
	   [target].[PrimaryPhone_B] = [source].[PrimaryPhone_B],
	   [target].[SecondaryPhone_A] = [source].[SecondaryPhone_A],
	   [target].[SecondaryPhone_B] = [source].[SecondaryPhone_B],
	   [target].[Email_A] = [source].[Email_A],
	   [target].[Email_B] = [source].[Email_B],
	   [target].[FirstName_LastName_DOB] = [source].[FirstName_LastName_DOB],
	   [target].[FirstName_LastName_DOB_Zip3] = [source].[FirstName_LastName_DOB_Zip3],
	   [target].[FirstName_LastName_Address_Zip3] = [source].[FirstName_LastName_Address_Zip3],
	   [target].[PrimaryPhone_Address_Zip] = [source].[PrimaryPhone_Address_Zip],
	   [target].[Address_Zip_Email] = [source].[Address_Zip_Email],
	   [target].[DateModified] = GETDATE()
	WHEN NOT MATCHED THEN
	INSERT ([PatientID_A],
		[PatientID_B],
		[FirstName_A],
		[LastName_A],
		[FirstName_B],
		[LastName_B],
		[DOB_A],
		[DOB_B],
		[Zip_A],
		[Zip_B],
		[Address1_A],
		[Address1_B],
		[PrimaryPhone_A],
		[PrimaryPhone_B],
		[SecondaryPhone_A],
		[SecondaryPhone_B],
		[Email_A],
		[Email_B],
		[FirstName_LastName_DOB],
		[FirstName_LastName_DOB_Zip3],
		[FirstName_LastName_Address_Zip3],
		[PrimaryPhone_Address_Zip],
		[Address_Zip_Email],
		[JW_Batch_ID])
	VALUES ([source].[PatientID_A],
		[source].[PatientID_B],
		[source].[FirstName_A],
		[source].[LastName_A],
		[source].[FirstName_B],
		[source].[LastName_B],
		[source].[DOB_A],
		[source].[DOB_B],
		[source].[Zip_A],
		[source].[Zip_B],
		[source].[Address1_A],
		[source].[Address1_B],
		[source].[PrimaryPhone_A],
		[source].[PrimaryPhone_B],
		[source].[SecondaryPhone_A],
		[source].[SecondaryPhone_B],
		[source].[Email_A],
		[source].[Email_B],
		[source].[FirstName_LastName_DOB],
		[source].[FirstName_LastName_DOB_Zip3],
		[source].[FirstName_LastName_Address_Zip3],
		[source].[PrimaryPhone_Address_Zip],
		[source].[Address_Zip_Email],
		@JW_Batch_ID);

	MERGE [dbo].[tblJaroWinkler_Universe] as [target]
	USING (
		SELECT 	[PatientID_A],
			[PatientID_B],
			[FirstName_A],
			[LastName_A],
			[FirstName_B],
			[LastName_B],
			[DOB_A],
			[DOB_B],
			[Zip_A],
			[Zip_B],
			[Address1_A],
			[Address1_B],
			[PrimaryPhone_A],
			[PrimaryPhone_B],
			[SecondaryPhone_A],
			[SecondaryPhone_B],
			[Email_A],
			[Email_B],
			[FirstName_LastName_DOB],
			[FirstName_LastName_DOB_Zip3],
			[FirstName_LastName_Address_Zip3],
			[PrimaryPhone_Address_Zip],
			[Address_Zip_Email]
		FROM
		(
		SELECT DENSE_RANK() OVER(PARTITION BY Ranking_By_IDs ORDER BY [PatientID_A] ASC) as Partitioning_By_IDs, *
		FROM (
			SELECT DENSE_RANK() OVER(ORDER BY cast(tblA.PatientID as bigint)*cast(tblB.PatientID as bigint)) as Ranking_By_IDs,
				tblA.PatientID as PatientID_A,
				tblB.PatientID as PatientID_B,
				tblA.[FirstName] as FirstName_A,
				tblA.[LastName] as LastName_A,
				tblB.[FirstName] as FirstName_B,
				tblB.[LastName] as LastName_B,
				tblA.[DOB] as DOB_A,
				tblB.[DOB] as DOB_B,
				tblA.[Zip] as Zip_A,
				tblB.[Zip] as Zip_B,
				tblA.[Address1] as Address1_A,
				tblB.[Address1] as Address1_B,
				tblA.[PrimaryPhone] as PrimaryPhone_A,
				tblB.[PrimaryPhone] as PrimaryPhone_B,
				tblA.[SecondaryPhone] as SecondaryPhone_A,
				tblB.[SecondaryPhone] as SecondaryPhone_B,
				tblA.[Email] as Email_A,
				tblB.[Email] as Email_B,
				[dbo].[fn_CLR_Fuzzy_JaroWinkler](ISNULL(tblA.[FirstName],'') + ISNULL(tblA.[LastName],'') + [dbo].[fn_StripPatternFromString](tblA.[DOB],'[A-Za-z0-9]'), ISNULL(tblB.[FirstName],'') + ISNULL(tblB.[LastName],'') + [dbo].[fn_StripPatternFromString](tblB.[DOB],'[A-Za-z0-9]')) as FirstName_LastName_DOB,
				[dbo].[fn_CLR_Fuzzy_JaroWinkler](ISNULL(tblA.[FirstName],'') + ISNULL(tblA.[LastName],'') + [dbo].[fn_StripPatternFromString](tblA.[DOB],'[A-Za-z0-9]') + [dbo].[fn_StripPatternFromString](LEFT(tblA.[Zip],3),'[A-Za-z0-9]'), ISNULL(tblB.[FirstName],'') + ISNULL(tblB.[LastName],'') + [dbo].[fn_StripPatternFromString](tblB.[DOB],'[A-Za-z0-9]') + [dbo].[fn_StripPatternFromString](LEFT(tblB.[Zip],3),'[A-Za-z0-9]')) as FirstName_LastName_DOB_Zip3,
				[dbo].[fn_CLR_Fuzzy_JaroWinkler](ISNULL(tblA.[FirstName],'') + ISNULL(tblA.[LastName],'') + [dbo].[fn_StripPatternFromString](tblA.[Address1],'[A-Za-z0-9]') + [dbo].[fn_StripPatternFromString](LEFT(tblA.[Zip],3),'[A-Za-z0-9]'), ISNULL(tblA.[FirstName],'') + ISNULL(tblA.[LastName],'') + [dbo].[fn_StripPatternFromString](tblB.[Address1],'[A-Za-z0-9]') + [dbo].[fn_StripPatternFromString](LEFT(tblB.[Zip],3),'[A-Za-z0-9]')) as FirstName_LastName_Address_Zip3,
				[dbo].[fn_CLR_Fuzzy_JaroWinkler]([dbo].[fn_StripPatternFromString](tblA.[PrimaryPhone],'[A-Za-z0-9]') + [dbo].[fn_StripPatternFromString](tblA.[Address1],'[A-Za-z0-9]') + [dbo].[fn_StripPatternFromString](tblA.[Zip],'[A-Za-z0-9]'), [dbo].[fn_StripPatternFromString](tblB.[PrimaryPhone],'[A-Za-z0-9]') + [dbo].[fn_StripPatternFromString](tblB.[Address1],'[A-Za-z0-9]') + [dbo].[fn_StripPatternFromString](tblB.[Zip],'[A-Za-z0-9]')) as PrimaryPhone_Address_Zip,
				[dbo].[fn_CLR_Fuzzy_JaroWinkler]([dbo].[fn_StripPatternFromString](tblA.[Address1],'[A-Za-z0-9]') + [dbo].[fn_StripPatternFromString](tblA.[Zip],'[A-Za-z0-9]') + [dbo].[fn_StripPatternFromString](tblA.[Email],'[A-Za-z0-9]'), [dbo].[fn_StripPatternFromString](tblB.[Address1],'[A-Za-z0-9]') + [dbo].[fn_StripPatternFromString](tblB.[Zip],'[A-Za-z0-9]') + [dbo].[fn_StripPatternFromString](tblB.[Email],'[A-Za-z0-9]')) as Address_Zip_Email
			FROM [dbo].[tblPatientInfo] tblA
				CROSS JOIN
				(
					SELECT PatientID
						  ,[FirstName]
						  ,[LastName]
						  ,[Address1]
						  ,[Zip]
						  ,[PrimaryPhone]
						  ,[SecondaryPhone]
						  ,[Email]
						  ,[DOB]
					FROM [dbo].[tblPatientInfo] with (nolock)
  					WHERE [ActiveFlag] = 1
						AND [NeedToRunJaroWinkler] = 1
				) tblB
			WHERE tblA.[ActiveFlag] = 1
				AND tblA.[NeedToRunJaroWinkler] = 1
				AND tblA.[PatientID] != tblB.[PatientID]
				AND [dbo].[fn_CLR_Fuzzy_JaroWinkler](ISNULL(tblA.[FirstName],'') + ISNULL(tblA.[LastName],'') + [dbo].[fn_StripPatternFromString](tblA.[DOB],'[A-Za-z0-9]') + [dbo].[fn_StripPatternFromString](LEFT(tblA.[Zip],3),'[A-Za-z0-9]'), ISNULL(tblB.[FirstName],'') + ISNULL(tblB.[LastName],'') + [dbo].[fn_StripPatternFromString](tblB.[DOB],'[A-Za-z0-9]') + [dbo].[fn_StripPatternFromString](LEFT(tblB.[Zip],3),'[A-Za-z0-9]')) >= @jw_threshold
		) tmp1
		) tmp2
		WHERE Partitioning_By_IDs = 1
	) as [source]
	ON ([target].[PatientID_A] = [source].PatientID_A AND [target].[PatientID_B] = [source].PatientID_B)
	WHEN MATCHED THEN
	UPDATE SET
	   [target].[FirstName_A] = [source].[FirstName_A],
	   [target].[LastName_A] = [source].[LastName_A],
	   [target].[FirstName_B] = [source].[FirstName_B],
	   [target].[LastName_B] = [source].[LastName_B],
	   [target].[DOB_A] = [source].[DOB_A],
	   [target].[DOB_B] = [source].[DOB_B],
	   [target].[Zip_A] = [source].[Zip_A],
	   [target].[Zip_B] = [source].[Zip_B],
	   [target].[Address1_A] = [source].[Address1_A],
	   [target].[Address1_B] = [source].[Address1_B],
	   [target].[PrimaryPhone_A] = [source].[PrimaryPhone_A],
	   [target].[PrimaryPhone_B] = [source].[PrimaryPhone_B],
	   [target].[SecondaryPhone_A] = [source].[SecondaryPhone_A],
	   [target].[SecondaryPhone_B] = [source].[SecondaryPhone_B],
	   [target].[Email_A] = [source].[Email_A],
	   [target].[Email_B] = [source].[Email_B],
	   [target].[FirstName_LastName_DOB] = [source].[FirstName_LastName_DOB],
	   [target].[FirstName_LastName_DOB_Zip3] = [source].[FirstName_LastName_DOB_Zip3],
	   [target].[FirstName_LastName_Address_Zip3] = [source].[FirstName_LastName_Address_Zip3],
	   [target].[PrimaryPhone_Address_Zip] = [source].[PrimaryPhone_Address_Zip],
	   [target].[Address_Zip_Email] = [source].[Address_Zip_Email],
	   [target].[DateModified] = GETDATE()
	WHEN NOT MATCHED THEN
	INSERT ([PatientID_A],
		[PatientID_B],
		[FirstName_A],
		[LastName_A],
		[FirstName_B],
		[LastName_B],
		[DOB_A],
		[DOB_B],
		[Zip_A],
		[Zip_B],
		[Address1_A],
		[Address1_B],
		[PrimaryPhone_A],
		[PrimaryPhone_B],
		[SecondaryPhone_A],
		[SecondaryPhone_B],
		[Email_A],
		[Email_B],
		[FirstName_LastName_DOB],
		[FirstName_LastName_DOB_Zip3],
		[FirstName_LastName_Address_Zip3],
		[PrimaryPhone_Address_Zip],
		[Address_Zip_Email],
		[JW_Batch_ID])
	VALUES ([source].[PatientID_A],
		[source].[PatientID_B],
		[source].[FirstName_A],
		[source].[LastName_A],
		[source].[FirstName_B],
		[source].[LastName_B],
		[source].[DOB_A],
		[source].[DOB_B],
		[source].[Zip_A],
		[source].[Zip_B],
		[source].[Address1_A],
		[source].[Address1_B],
		[source].[PrimaryPhone_A],
		[source].[PrimaryPhone_B],
		[source].[SecondaryPhone_A],
		[source].[SecondaryPhone_B],
		[source].[Email_A],
		[source].[Email_B],
		[source].[FirstName_LastName_DOB],
		[source].[FirstName_LastName_DOB_Zip3],
		[source].[FirstName_LastName_Address_Zip3],
		[source].[PrimaryPhone_Address_Zip],
		[source].[Address_Zip_Email],
		@JW_Batch_ID);

	UPDATE [dbo].[tblPatientInfo]
		SET [NeedToRunJaroWinkler] = 0
	WHERE [NeedToRunJaroWinkler] = 1

END TRY
BEGIN CATCH

	DECLARE @errmsg   nvarchar(2048),
			@severity tinyint,
			@state    tinyint,
			@errno    int,
			@proc     sysname,
			@lineno   int

	SELECT @errmsg = error_message(), @severity = error_severity(),
			@state  = error_state(), @errno = error_number(),
			@proc   = error_procedure(), @lineno = error_line()

	IF @errmsg NOT LIKE '***%'
	BEGIN
		SELECT @errmsg = '*** ' + coalesce(quotename(@proc), '<dynamic SQL>') +
						', Line ' + ltrim(str(@lineno)) + '. Errno ' +
						ltrim(str(@errno)) + ': ' + @errmsg
	END
	RAISERROR('%s', @severity, @state, @errmsg)

END CATCH
