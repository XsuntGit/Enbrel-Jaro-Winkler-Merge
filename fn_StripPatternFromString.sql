USE [Enbrel_Production]
GO
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
CREATE OR ALTER FUNCTION [dbo].[fn_StripPatternFromString]
(
  @s VARCHAR(255),
  @pattern VARCHAR(255)
)
RETURNS VARCHAR(255)
AS
BEGIN
  DECLARE @p INT = 1, @n VARCHAR(255) = '';
  WHILE @p <= LEN(@s)
  BEGIN
    IF SUBSTRING(@s, @p, 1) LIKE @pattern
    BEGIN
      SET @n += SUBSTRING(@s, @p, 1);
    END
    SET @p += 1;
  END
  RETURN(@n);
END
