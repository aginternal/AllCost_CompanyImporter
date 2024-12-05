using DbImporterAllCost.Dto;
using Microsoft.Data.SqlClient;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Linq.Expressions;
using System.Reflection.Metadata.Ecma335;
using System.Text;
using System.Threading.Tasks;

namespace DbImporterAllCost.Utils
{
    public class PriImporter
    {
        public string PriName { get; set; }
        public string OurName { get; set; }

        public PriImporter(string priName, string ourName)
        {
            this.PriName = priName;
            this.OurName = ourName;
        }

        public void ImportAllData(string connStr)
        {
            //artigos
            ImportIvas(connStr);
            ImportFamilias(connStr);
            ImportSubFamilias(connStr);
            ImportUnidades(connStr);
            ImportTiposArtigo(connStr);
            ImportArmazens(connStr);
            ImportMarcas(connStr);
            ImportModelos(connStr);
            ImportComposicoes(connStr);
            ImportClassificacoes(connStr);
            ImportTiposCor(connStr);
            ImportCores(connStr);
            ImportCertificacoes(connStr);
            ImportTiposTamanho(connStr);
            ImportTamanhos(connStr);
            ImportGramagens(connStr);
            ImportArtigos(connStr);

            //entidades
            ImportMotivosIsencao(connStr);
            ImportPaises(connStr);
            ImportCondicoesPagamento(connStr);
            ImportDocumentosBancos(connStr);
            ImportEntidades(connStr);

            //docs
            ImportArtigoMoedas(connStr);
            ImportTiposDocumento(connStr);
            ImportSeries(connStr);
            ImportDocumentos(connStr);
            ImportDocumentosDetalhe(connStr);
        }

        private void ImportIvas(string connStr)
        {
            string query = $@"
                BEGIN TRANSACTION;

                TRUNCATE TABLE [{this.OurName}].[dbo].[CRM_TAXAS_IVA]

                INSERT INTO [{this.OurName}].[dbo].[CRM_TAXAS_IVA] (Codigo, Taxa, Descricao)
                SELECT * FROM OPENQUERY([192.168.1.221, 58681\SVSQL\PRILEV100], 'select Iva, Taxa, Descricao from [{this.PriName}].[dbo].[Iva]')

                COMMIT TRANSACTION;
            ";

            try
            {
                using (SqlConnection connection = new SqlConnection(connStr))
                {
                    connection.Open();

                    using (SqlCommand command = new SqlCommand(query, connection))
                    {
                        command.CommandTimeout = 0;
                        command.ExecuteNonQuery();
                    }

                    connection.Close();
                }

                var log = new DbImportationLog("CRM_TAXAS_IVA", "SUCCESS", "");
                log.Commit(connStr);
            }
            catch (Exception ex)
            {
                var log = new DbImportationLog("CRM_TAXAS_IVA", "FAILED", ex.Message);
                log.Commit(connStr);
            }
        }

        private void ImportFamilias(string connStr)
        {
            string query = $@"
                BEGIN TRANSACTION;

                TRUNCATE TABLE [{this.OurName}].[dbo].[CRM_FAMILIAS]
                
                INSERT INTO [{this.OurName}].[dbo].[CRM_FAMILIAS] (Familia, Descricao, PermiteDevolucao)
                SELECT * FROM OPENQUERY([192.168.1.221, 58681\SVSQL\PRILEV100], 'select Familia, Descricao, PermiteDevolucao from [{this.PriName}].[dbo].[Familias]')

                COMMIT TRANSACTION;
            ";

            try
            {
                using (SqlConnection connection = new SqlConnection(connStr))
                {
                    connection.Open();

                    using (SqlCommand command = new SqlCommand(query, connection))
                    {
                        command.CommandTimeout = 0;
                        command.ExecuteNonQuery();
                    }

                    connection.Close();
                }

                var log = new DbImportationLog("CRM_FAMILIAS", "SUCCESS", "");
                log.Commit(connStr);
            }
            catch (Exception ex)
            {
                var log = new DbImportationLog("CRM_FAMILIAS", "FAILED", ex.Message);
                log.Commit(connStr);
            }
        }

        private void ImportSubFamilias(string connStr)
        {
            string query = $@"
                BEGIN TRANSACTION;

                TRUNCATE TABLE [{this.OurName}].[dbo].[CRM_SUBFAMILIAS]

                INSERT INTO [{this.OurName}].[dbo].[CRM_SUBFAMILIAS] (SubFamilia, Familia, Descricao, PermiteDevolucao, FamiliaId)
                SELECT SF.*, F.Id 
                FROM OPENQUERY([192.168.1.221, 58681\SVSQL\PRILEV100], 'select SubFamilia, Familia, Descricao, PermiteDevolucao from [{this.PriName}].[dbo].[SubFamilias]') as SF
                inner join [{this.OurName}].[dbo].[CRM_FAMILIAS] F on F.Familia = SF.Familia

                COMMIT TRANSACTION;
            ";

            try
            {
                using (SqlConnection connection = new SqlConnection(connStr))
                {
                    connection.Open();

                    using (SqlCommand command = new SqlCommand(query, connection))
                    {
                        command.CommandTimeout = 0;
                        command.ExecuteNonQuery();
                    }

                    connection.Close();
                }

                var log = new DbImportationLog("CRM_SUBFAMILIAS", "SUCCESS", "");
                log.Commit(connStr);
            }
            catch (Exception ex)
            {
                var log = new DbImportationLog("CRM_SUBFAMILIAS", "FAILED", ex.Message);
                log.Commit(connStr);
            }
        }

        private void ImportUnidades(string connStr)
        {
            string query = $@"
                BEGIN TRANSACTION;

                TRUNCATE TABLE [{this.OurName}].[dbo].[CRM_UNIDADES]
                
                INSERT INTO [{this.OurName}].[dbo].[CRM_UNIDADES](Un, Nome)
                SELECT * FROM OPENQUERY([192.168.1.221, 58681\SVSQL\PRILEV100], 'select Unidade, Descricao from [{this.PriName}].[dbo].[Unidades]')

                COMMIT TRANSACTION;
            ";

            try
            {
                using (SqlConnection connection = new SqlConnection(connStr))
                {
                    connection.Open();

                    using (SqlCommand command = new SqlCommand(query, connection))
                    {
                        command.CommandTimeout = 0;
                        command.ExecuteNonQuery();
                    }

                    connection.Close();
                }

                var log = new DbImportationLog("CRM_UNIDADES", "SUCCESS", "");
                log.Commit(connStr);
            }
            catch (Exception ex)
            {
                var log = new DbImportationLog("CRM_UNIDADES", "FAILED", ex.Message);
                log.Commit(connStr);
            }
        }

        private void ImportTiposArtigo(string connStr)
        {
            //marquei como serviço os tipos com 'serviço' no nome
            string query = $@"
                BEGIN TRANSACTION;

                TRUNCATE TABLE [{this.OurName}].[dbo].[CRM_TIPOS_ARTIGO]
                
                INSERT INTO [{this.OurName}].[dbo].[CRM_TIPOS_ARTIGO](Codigo, Tipo, IsServico)
                SELECT * FROM OPENQUERY([192.168.1.221, 58681\SVSQL\PRILEV100], 'select TipoArtigo, Descricao, 0 from [{this.PriName}].[dbo].[TiposArtigo]')
                UPDATE [{this.OurName}].[dbo].[CRM_TIPOS_ARTIGO] SET IsServico = 1 WHERE Codigo = 0 or Codigo = 1 or Codigo = 2

                COMMIT TRANSACTION;
            ";

            try
            {
                using (SqlConnection connection = new SqlConnection(connStr))
                {
                    connection.Open();

                    using (SqlCommand command = new SqlCommand(query, connection))
                    {
                        command.CommandTimeout = 0;
                        command.ExecuteNonQuery();
                    }

                    connection.Close();
                }

                var log = new DbImportationLog("CRM_TIPOS_ARTIGO", "SUCCESS", "");
                log.Commit(connStr);
            }
            catch (Exception ex)
            {
                var log = new DbImportationLog("CRM_TIPOS_ARTIGO", "FAILED", ex.Message);
                log.Commit(connStr);
            }
        }

        private void ImportArmazens(string connStr)
        {
            string query = $@"
                BEGIN TRANSACTION;

                TRUNCATE TABLE [{this.OurName}].[dbo].[CRM_ARMAZEM]
                
                INSERT INTO [{this.OurName}].[dbo].[CRM_ARMAZEM](NOME, CODIGOERP, ERPID, PORDEFEITO, NIVEL, ARMAZEM_PAI, ISLIXO, ISUSADO, LOJAID)
                SELECT * FROM OPENQUERY([192.168.1.221, 58681\SVSQL\PRILEV100], 'select a.Descricao, a.Armazem as Codigo, a.Armazem, 0, null, null, 0, CASE WHEN EXISTS ( SELECT 1 FROM [{this.PriName}].[dbo].[LinhasDoc] ld WHERE ld.Armazem like a.Armazem union all SELECT 1 FROM [{this.PriName}].[dbo].[LinhasCompras] ld WHERE ld.Armazem like a.Armazem union all SELECT 1 FROM [{this.PriName}].[dbo].[LinhasInternos] ld WHERE ld.Armazem like a.Armazem) THEN 1 ELSE 0 END AS IsUsado, 0 from [{this.PriName}].[dbo].[Armazens] a')

                COMMIT TRANSACTION;
            ";

            try
            {
                using (SqlConnection connection = new SqlConnection(connStr))
                {
                    connection.Open();

                    using (SqlCommand command = new SqlCommand(query, connection))
                    {
                        command.CommandTimeout = 0;
                        command.ExecuteNonQuery();
                    }

                    connection.Close();
                }

                var log = new DbImportationLog("CRM_ARMAZEM", "SUCCESS", "");
                log.Commit(connStr);
            }
            catch (Exception ex)
            {
                var log = new DbImportationLog("CRM_ARMAZEM", "FAILED", ex.Message);
                log.Commit(connStr);
            }
        }

        private void ImportMarcas(string connStr)
        {
            string query = $@"
                BEGIN TRANSACTION;

                TRUNCATE TABLE [{this.OurName}].[dbo].[CRM_MARCAS]
                
                INSERT INTO [{this.OurName}].[dbo].[CRM_MARCAS](MARCA, DESCRICAO, CDU_Ativa)
                SELECT * FROM OPENQUERY([192.168.1.221, 58681\SVSQL\PRILEV100], 'select Marca, Descricao, CDU_Ativa from [{this.PriName}].[dbo].[Marcas]')

                COMMIT TRANSACTION;
            ";

            try
            {
                using (SqlConnection connection = new SqlConnection(connStr))
                {
                    connection.Open();

                    using (SqlCommand command = new SqlCommand(query, connection))
                    {
                        command.CommandTimeout = 0;
                        command.ExecuteNonQuery();
                    }

                    connection.Close();
                }

                var log = new DbImportationLog("CRM_MARCAS", "SUCCESS", "");
                log.Commit(connStr);
            }
            catch (Exception ex)
            {
                var log = new DbImportationLog("CRM_MARCAS", "FAILED", ex.Message);
                log.Commit(connStr);
            }
        }

        private void ImportModelos(string connStr)
        {
            string query = $@"
                BEGIN TRANSACTION;

                TRUNCATE TABLE [{this.OurName}].[dbo].[CRM_MODELOS]
                
                INSERT INTO [{this.OurName}].[dbo].[CRM_MODELOS](Marca, Modelo, Descricao, Ordem, CDU_ATIVO, MarcaId)
                SELECT MO.*, MA.Id FROM
                OPENQUERY([192.168.1.221, 58681\SVSQL\PRILEV100], 'select Marca, Modelo, Descricao, Ordem, CDU_ATIVO from [{this.PriName}].[dbo].[Modelos]') as MO
                left join [{this.OurName}].[dbo].[CRM_MARCAS] MA on MA.Marca = MO.Marca

                COMMIT TRANSACTION;
            ";

            try
            {
                using (SqlConnection connection = new SqlConnection(connStr))
                {
                    connection.Open();

                    using (SqlCommand command = new SqlCommand(query, connection))
                    {
                        command.CommandTimeout = 0;
                        command.ExecuteNonQuery();
                    }

                    connection.Close();
                }

                var log = new DbImportationLog("CRM_MODELOS", "SUCCESS", "");
                log.Commit(connStr);
            }
            catch (Exception ex)
            {
                var log = new DbImportationLog("CRM_MODELOS", "FAILED", ex.Message);
                log.Commit(connStr);
            }
        }

        private void ImportComposicoes(string connStr)
        {
            string query = $@"
                BEGIN TRANSACTION;

                TRUNCATE TABLE [{this.OurName}].[dbo].[CRM_COMPOSICOES]
                
                INSERT INTO [{this.OurName}].[dbo].[CRM_COMPOSICOES](Codigo, Descricao)
                SELECT * FROM OPENQUERY([192.168.1.221, 58681\SVSQL\PRILEV100], 'select CDU_BC_Codigo, CDU_BC_Descricao from [{this.PriName}].[dbo].[TDU_BC_Composicoes]')

                COMMIT TRANSACTION;
            ";

            try
            {
                using (SqlConnection connection = new SqlConnection(connStr))
                {
                    connection.Open();

                    using (SqlCommand command = new SqlCommand(query, connection))
                    {
                        command.CommandTimeout = 0;
                        command.ExecuteNonQuery();
                    }

                    connection.Close();
                }

                var log = new DbImportationLog("CRM_COMPOSICOES", "SUCCESS", "");
                log.Commit(connStr);
            }
            catch (Exception ex)
            {
                var log = new DbImportationLog("CRM_COMPOSICOES", "FAILED", ex.Message);
                log.Commit(connStr);
            }
        }

        private void ImportClassificacoes(string connStr)
        {
            string query = $@"
                BEGIN TRANSACTION;

                TRUNCATE TABLE [{this.OurName}].[dbo].[CRM_CLASSIFICACOES]
                
                INSERT INTO [{this.OurName}].[dbo].[CRM_CLASSIFICACOES](Classificacao, Descricao, Codigo, TrataLotes, TipoArtigoAssoc, FamiliaAssoc, SubFamiliaAssoc, MarcaAssoc, ModeloAssoc, ArmazemAssoc, UnidadeAssoc)
                SELECT CLA.Modelo, CLA.Descricao, CLA.Codigo, CLA.TrataLotes, TA.Id, F.Id, SF.Id, MA.Id, MO.Id, A.ID, U.Id FROM 
                OPENQUERY([192.168.1.221, 58681\SVSQL\PRILEV100], 
                'select Modelo, Descricao, Codigo, TrataLotes, TipoArtigoAssoc, FamiliaAssoc, SubFamiliaAssoc, MarcaAssoc, ModeloAssoc, ArmazemAssoc, UnidadeAssoc from [{this.PriName}].[dbo].[VMP_ART_GA_Modelos]') as CLA
                left join [{this.OurName}].[dbo].[CRM_TIPOS_ARTIGO] TA on TA.Codigo = CLA.TipoArtigoAssoc
                left join [{this.OurName}].[dbo].[CRM_FAMILIAS] F on F.Familia = CLA.FamiliaAssoc
                left join [{this.OurName}].[dbo].[CRM_SUBFAMILIAS] SF on SF.SubFamilia = CLA.SubFamiliaAssoc
                left join [{this.OurName}].[dbo].[CRM_MARCAS] MA on MA.Marca = CLA.MarcaAssoc
                left join [{this.OurName}].[dbo].[CRM_MODELOS] MO on MO.Modelo = CLA.ModeloAssoc
                left join [{this.OurName}].[dbo].[CRM_ARMAZEM] A on A.CODIGOERP = CLA.ArmazemAssoc
                left join [{this.OurName}].[dbo].[CRM_UNIDADES] U on U.Un = CLA.UnidadeAssoc

                COMMIT TRANSACTION;
            ";

            try
            {
                using (SqlConnection connection = new SqlConnection(connStr))
                {
                    connection.Open();

                    using (SqlCommand command = new SqlCommand(query, connection))
                    {
                        command.CommandTimeout = 0;
                        command.ExecuteNonQuery();
                    }

                    connection.Close();
                }

                var log = new DbImportationLog("CRM_CLASSIFICACOES", "SUCCESS", "");
                log.Commit(connStr);
            }
            catch (Exception ex)
            {
                var log = new DbImportationLog("CRM_CLASSIFICACOES", "FAILED", ex.Message);
                log.Commit(connStr);
            }
        }

        private void ImportTiposCor(string connStr)
        {
            string query = $@"
                BEGIN TRANSACTION;

                TRUNCATE TABLE [{this.OurName}].[dbo].[CRM_TIPOS_COR]
                
                INSERT INTO [{this.OurName}].[dbo].[CRM_TIPOS_COR](Codigo, Descricao)
                SELECT * FROM OPENQUERY([192.168.1.221, 58681\SVSQL\PRILEV100], 'select Dimensao, Descricao from [{this.PriName}].[dbo].[Dimensao] where TipoDim = ''201'' or TipoDim = ''COR''')

                COMMIT TRANSACTION;
            ";

            try
            {
                using (SqlConnection connection = new SqlConnection(connStr))
                {
                    connection.Open();

                    using (SqlCommand command = new SqlCommand(query, connection))
                    {
                        command.CommandTimeout = 0;
                        command.ExecuteNonQuery();
                    }

                    connection.Close();
                }

                var log = new DbImportationLog("CRM_TIPOS_COR", "SUCCESS", "");
                log.Commit(connStr);
            }
            catch (Exception ex)
            {
                var log = new DbImportationLog("CRM_TIPOS_COR", "FAILED", ex.Message);
                log.Commit(connStr);
            }
        }

        private void ImportCores(string connStr)
        {
            string query = $@"
                BEGIN TRANSACTION;

                TRUNCATE TABLE [{this.OurName}].[dbo].[CRM_CORES]
                
                INSERT INTO [{this.OurName}].[dbo].[CRM_CORES](Codigo, Ordem, Descricao, TipoCorId, TipoCor)
                SELECT C.RubDim, C.Ordem, C.Descricao, TC.Id, TC.Codigo FROM 
                OPENQUERY([192.168.1.221, 58681\SVSQL\PRILEV100], 'select LD.RubDim, LD.Ordem, LD.Descricao, LD.Dimensao from [{this.PriName}].[dbo].[Dimensao] D inner join [{this.PriName}].[dbo].[LinhasDimensao] LD on LD.Dimensao = D.Dimensao where TipoDim = ''201'' or TipoDim = ''COR''') C
                left join [{this.OurName}].[dbo].[CRM_TIPOS_COR] TC on C.Dimensao = TC.Codigo

                COMMIT TRANSACTION;
            ";

            try
            {
                using (SqlConnection connection = new SqlConnection(connStr))
                {
                    connection.Open();

                    using (SqlCommand command = new SqlCommand(query, connection))
                    {
                        command.CommandTimeout = 0;
                        command.ExecuteNonQuery();
                    }

                    connection.Close();
                }

                var log = new DbImportationLog("CRM_CORES", "SUCCESS", "");
                log.Commit(connStr);
            }
            catch (Exception ex)
            {
                var log = new DbImportationLog("CRM_CORES", "FAILED", ex.Message);
                log.Commit(connStr);
            }
        }

        private void ImportCertificacoes(string connStr)
        {
            string query = $@"
                BEGIN TRANSACTION;

                TRUNCATE TABLE [{this.OurName}].[dbo].[CRM_CRETIFICACOES]
                
                INSERT INTO [{this.OurName}].[dbo].[CRM_CRETIFICACOES](ErpId, Codigo, Descricao)
                SELECT * FROM OPENQUERY([192.168.1.221, 58681\SVSQL\PRILEV100], 'select Id, Codigo, Descricao from [{this.PriName}].[dbo].[VMP_ART_GA_CategoriasDetalhe] where Categoria = ''BC_CERT''')

                COMMIT TRANSACTION;
            ";

            try
            {
                using (SqlConnection connection = new SqlConnection(connStr))
                {
                    connection.Open();

                    using (SqlCommand command = new SqlCommand(query, connection))
                    {
                        command.CommandTimeout = 0;
                        command.ExecuteNonQuery();
                    }

                    connection.Close();
                }

                var log = new DbImportationLog("CRM_CRETIFICACOES", "SUCCESS", "");
                log.Commit(connStr);
            }
            catch (Exception ex)
            {
                var log = new DbImportationLog("CRM_CRETIFICACOES", "FAILED", ex.Message);
                log.Commit(connStr);
            }
        }

        private void ImportTiposTamanho(string connStr)
        {
            string query = $@"
                BEGIN TRANSACTION;

                TRUNCATE TABLE [{this.OurName}].[dbo].[CRM_TIPOS_TAMANHO]
                
                INSERT INTO [{this.OurName}].[dbo].[CRM_TIPOS_TAMANHO](Codigo, Descricao)
                SELECT * FROM OPENQUERY([192.168.1.221, 58681\SVSQL\PRILEV100], 'select Dimensao, Descricao from [{this.PriName}].[dbo].[Dimensao] D where TipoDim = ''200'' or TipoDim = ''TAM''')

                COMMIT TRANSACTION;
            ";

            try
            {
                using (SqlConnection connection = new SqlConnection(connStr))
                {
                    connection.Open();

                    using (SqlCommand command = new SqlCommand(query, connection))
                    {
                        command.CommandTimeout = 0;
                        command.ExecuteNonQuery();
                    }

                    connection.Close();
                }

                var log = new DbImportationLog("CRM_TIPOS_TAMANHO", "SUCCESS", "");
                log.Commit(connStr);
            }
            catch (Exception ex)
            {
                var log = new DbImportationLog("CRM_TIPOS_TAMANHO", "FAILED", ex.Message);
                log.Commit(connStr);
            }
        }

        private void ImportTamanhos(string connStr)
        {
            string query = $@"
                BEGIN TRANSACTION;

                TRUNCATE TABLE [{this.OurName}].[dbo].[CRM_TAMANHOS]
                
                INSERT INTO [{this.OurName}].[dbo].[CRM_TAMANHOS](Codigo, Ordem, Descricao, TipoTamanhoId, TipoTamanho)
                SELECT T.RubDim, T.Ordem, T.Descricao, TT.Id, TT.Codigo FROM 
                OPENQUERY([192.168.1.221, 58681\SVSQL\PRILEV100], 'select LD.RubDim, LD.Ordem, LD.Descricao, LD.Dimensao from [{this.PriName}].[dbo].[Dimensao] D inner join [{this.PriName}].[dbo].[LinhasDimensao] LD on LD.Dimensao = D.Dimensao where TipoDim = ''200'' or TipoDim = ''TAM''') T
                left join [{this.OurName}].[dbo].[CRM_TIPOS_TAMANHO] TT on TT.Codigo = T.Dimensao

                COMMIT TRANSACTION;
            ";

            try
            {
                using (SqlConnection connection = new SqlConnection(connStr))
                {
                    connection.Open();

                    using (SqlCommand command = new SqlCommand(query, connection))
                    {
                        command.CommandTimeout = 0;
                        command.ExecuteNonQuery();
                    }

                    connection.Close();
                }

                var log = new DbImportationLog("CRM_TAMANHOS", "SUCCESS", "");
                log.Commit(connStr);
            }
            catch (Exception ex)
            {
                var log = new DbImportationLog("CRM_TAMANHOS", "FAILED", ex.Message);
                log.Commit(connStr);
            }
        }

        private void ImportGramagens(string connStr)
        {
            string query = $@"
                BEGIN TRANSACTION;

                TRUNCATE TABLE [{this.OurName}].[dbo].[CRM_GRAMAGENS]
                
                INSERT INTO [{this.OurName}].[dbo].[CRM_GRAMAGENS](ErpId, Codigo, Descricao)
                SELECT * FROM OPENQUERY([192.168.1.221, 58681\SVSQL\PRILEV100], 'select Id, Codigo, Descricao from [{this.PriName}].[dbo].[VMP_ART_GA_CategoriasDetalhe] where Categoria = ''T.GRA''')

                COMMIT TRANSACTION;
            ";

            try
            {
                using (SqlConnection connection = new SqlConnection(connStr))
                {
                    connection.Open();

                    using (SqlCommand command = new SqlCommand(query, connection))
                    {
                        command.CommandTimeout = 0;
                        command.ExecuteNonQuery();
                    }

                    connection.Close();
                }

                var log = new DbImportationLog("CRM_GRAMAGENS", "SUCCESS", "");
                log.Commit(connStr);
            }
            catch (Exception ex)
            {
                var log = new DbImportationLog("CRM_GRAMAGENS", "FAILED", ex.Message);
                log.Commit(connStr);
            }
        }

        private void ImportArtigos(string connStr)
        {
            string query = $@"
                BEGIN TRANSACTION;

                DELETE FROM [{this.OurName}].[dbo].[CRM_ARTIGO]

                INSERT INTO [{this.OurName}].[dbo].[CRM_ARTIGO](codigo_artigo, artigo, artigo_pai, descricao, descricao_abreviada, ultimo_custo, iva_compra, iva_venda, iva_compra_id, iva_venda_id, stock,
                custo_padrao, preco_venda, data_upd, observacoes, tipo_artigo, tipo_artigo_id, unidade, unidade_id, isactive, stock_minimo, isservice, margem, preco_venda_liquido,
                tem_gestao, has_familia, familia, has_subfamilia, subfamilia, has_marca, marca, has_modelo, modelo, has_composicao, composicao, has_certificacao, certificacao,
                has_gramagem, gramagem, has_cor, cor, has_tamanho, tamanho, nome, has_tipo_tamanho, tipo_tamanho, has_tipo_cor, tipo_cor)
                SELECT A.Artigo, A.Familia, A.SubFamilia, A.Descricao, A.Descricao, A.PCUltimo, A.Iva, A.Iva, IVA.Id, IVA.Id, A.STKActual, A.PCPadrao, A.PCMedio, SYSDATETIME(), A.Observacoes, 
                TA.Tipo, TA.Id, A.UnidadeVenda, U.Id, case when A.ArtigoAnulado = 0 then 1 else 0 end, A.STKMinimo, case when A.TipoArtigo = 1 or A.TipoArtigo = 0 then 1 else 0 end,
                0, 0, case when A.MovStock = 'S' then 1 else 0 end, case when A.Familia is not null then 1 else 0 end, A.Familia, case when A.SubFamilia is not null then 1 else 0 end, A.SubFamilia, 
                case when A.Marca is not null then 1 else 0 end, A.Marca, case when A.Modelo is not null then 1 else 0 end, A.Modelo, 
                case when A.CDU_BC_Composicao is not null then 1 else 0 end, A.CDU_BC_Composicao, case when A.CDU_BC_Certificado is not null then 1 else 0 end, A.CDU_BC_Certificado, 
                case when A.CDU_GramagemM2 is not null then 1 else 0 end, A.CDU_GramagemM2, case when A.RubDim2 is not null then 1 else 0 end, A.RubDim2, 
                case when A.RubDim1 is not null then 1 else 0 end, A.RubDim1, A.Descricao, case when A.Dim1 is not null then 1 else 0 end, A.Dim1,
                case when A.Dim2 is not null then 1 else 0 end, A.Dim2
                FROM 
                OPENQUERY([192.168.1.221, 58681\SVSQL\PRILEV100], 'select A.Artigo, A.Descricao, A.Observacoes, A.Iva, A.ArtigoAnulado, A.STKMinimo, A.STKActual, A.Marca, A.Modelo, A.Ecovalor, A.PCMedio, A.PCUltimo, A.PCPadrao, A.TipoArtigo, A.SubFamilia, A.Familia, A.UnidadeVenda, A.MovStock, A.RubDim1, A.RubDim2, A.CDU_BC_Composicao, A.CDU_BC_Certificado, A.CDU_GramagemM2, A.Dim1, A.Dim2 from [{this.PriName}].[dbo].[Artigo] A ') A
                left join [{this.OurName}].[dbo].[CRM_TAXAS_IVA] IVA on IVA.Codigo = A.Iva
                left join [{this.OurName}].[dbo].[CRM_TIPOS_ARTIGO] TA on TA.Codigo = A.TipoArtigo
                left join [{this.OurName}].[dbo].[CRM_UNIDADES] U on U.Un = A.UnidadeVenda

                COMMIT TRANSACTION;
            ";

            try
            {
                using (SqlConnection connection = new SqlConnection(connStr))
                {
                    connection.Open();

                    using (SqlCommand command = new SqlCommand(query, connection))
                    {
                        command.CommandTimeout = 0;
                        command.ExecuteNonQuery();
                    }

                    connection.Close();
                }

                var log = new DbImportationLog("CRM_ARTIGO", "SUCCESS", "");
                log.Commit(connStr);
            }
            catch (Exception ex)
            {
                var log = new DbImportationLog("CRM_ARTIGO", "FAILED", ex.Message);
                log.Commit(connStr);
            }
        }

        private void ImportMotivosIsencao(string connStr)
        {
            string query = $@"
                BEGIN TRANSACTION;

                TRUNCATE TABLE [{this.OurName}].[dbo].[CRM_MOTIVOSISENCAO]
                
                INSERT INTO [{this.OurName}].[dbo].[CRM_MOTIVOSISENCAO](DESCRICAO, ERPID, CODIGOERP)
                SELECT * FROM OPENQUERY([192.168.1.221, 58681\SVSQL\PRILEV100], 'select Descricao, CodigoMotivoIsencao, CodigoMotivoIsencao as CMI from [{this.PriName}].[dbo].[MotivosIsencaoIVA]')

                COMMIT TRANSACTION;
            ";

            try
            {
                using (SqlConnection connection = new SqlConnection(connStr))
                {
                    connection.Open();

                    using (SqlCommand command = new SqlCommand(query, connection))
                    {
                        command.CommandTimeout = 0;
                        command.ExecuteNonQuery();
                    }

                    connection.Close();
                }

                var log = new DbImportationLog("CRM_MOTIVOSISENCAO", "SUCCESS", "");
                log.Commit(connStr);
            }
            catch (Exception ex)
            {
                var log = new DbImportationLog("CRM_MOTIVOSISENCAO", "FAILED", ex.Message);
                log.Commit(connStr);
            }
        }

        private void ImportPaises(string connStr)
        {
            string query = $@"
                BEGIN TRANSACTION;

                TRUNCATE TABLE [{this.OurName}].[dbo].[CRM_PAISES]
                
                INSERT INTO [{this.OurName}].[dbo].[CRM_PAISES](DESCRICAO, CODIGO, COUNTRID, MARKETTYPE, SAFTCODE)
                SELECT * FROM OPENQUERY([192.168.1.221, 58681\SVSQL\PRILEV100], 'select Descricao, Pais, Pais as P, 0, Pais as PP from [{this.PriName}].[dbo].[Paises]')

                COMMIT TRANSACTION;
            ";

            try
            {
                using (SqlConnection connection = new SqlConnection(connStr))
                {
                    connection.Open();

                    using (SqlCommand command = new SqlCommand(query, connection))
                    {
                        command.CommandTimeout = 0;
                        command.ExecuteNonQuery();
                    }

                    connection.Close();
                }

                var log = new DbImportationLog("CRM_PAISES", "SUCCESS", "");
                log.Commit(connStr);
            }
            catch (Exception ex)
            {
                var log = new DbImportationLog("CRM_PAISES", "FAILED", ex.Message);
                log.Commit(connStr);
            }
        }

        private void ImportCondicoesPagamento(string connStr)
        {
            string query = $@"
                BEGIN TRANSACTION;

                TRUNCATE TABLE [{this.OurName}].[dbo].[CRM_CONDICOES_PAGAMENTO]
                 
                INSERT INTO [{this.OurName}].[dbo].[CRM_CONDICOES_PAGAMENTO](Descricao, ErpiId, Codigo, NumDias)
                SELECT * FROM OPENQUERY([192.168.1.221, 58681\SVSQL\PRILEV100], 'select Descricao, CondPag as C, CondPag as CC, Dias from [{this.PriName}].[dbo].[CondPag]')

                COMMIT TRANSACTION;
            ";

            try
            {
                using (SqlConnection connection = new SqlConnection(connStr))
                {
                    connection.Open();

                    using (SqlCommand command = new SqlCommand(query, connection))
                    {
                        command.CommandTimeout = 0;
                        command.ExecuteNonQuery();
                    }

                    connection.Close();
                }

                var log = new DbImportationLog("CRM_CONDICOES_PAGAMENTO", "SUCCESS", "");
                log.Commit(connStr);
            }
            catch (Exception ex)
            {
                var log = new DbImportationLog("CRM_CONDICOES_PAGAMENTO", "FAILED", ex.Message);
                log.Commit(connStr);
            }
        }

        private void ImportDocumentosBancos(string connStr)
        {
            string query = $@"
                BEGIN TRANSACTION;

                TRUNCATE TABLE [{this.OurName}].[dbo].[CRM_DOCUMENTOS_BANCOS]
                
                INSERT INTO [{this.OurName}].[dbo].[CRM_DOCUMENTOS_BANCOS](CartaoCredito, ChequePreDatado, Descricao, ExportaN68, ExportaPS2, Movim, MovInterno, NumDias, Outro, Recibo, TipoCC, TipoCX, TipoDC, TipoDO, TipoDP, TipoMv, Vale)
                SELECT * FROM OPENQUERY([192.168.1.221, 58681\SVSQL\PRILEV100], 'select CartaoCredito, ChequePreDatado, Descricao, ExportaN68, ExportaPS2, Movim, MovInterno, NumDias, Outro, Recibo, TipoCC, TipoCX, TipoDC, TipoDO, TipoDP, TipoMv, Vale from [{this.PriName}].[dbo].[DocumentosBancos]')

                COMMIT TRANSACTION;
            ";

            try
            {
                using (SqlConnection connection = new SqlConnection(connStr))
                {
                    connection.Open();

                    using (SqlCommand command = new SqlCommand(query, connection))
                    {
                        command.CommandTimeout = 0;
                        command.ExecuteNonQuery();
                    }

                    connection.Close();
                }

                var log = new DbImportationLog("CRM_DOCUMENTOS_BANCOS", "SUCCESS", "");
                log.Commit(connStr);
            }
            catch (Exception ex)
            {
                var log = new DbImportationLog("CRM_DOCUMENTOS_BANCOS", "FAILED", ex.Message);
                log.Commit(connStr);
            }
        }

        private void ImportEntidades(string connStr)
        {
            string query = $@"
                BEGIN TRANSACTION;

                TRUNCATE TABLE [{this.OurName}].[dbo].[CRM_ENTIDADE]
                
                INSERT INTO [{this.OurName}].[dbo].[CRM_ENTIDADE](codigo, nome, nome_fiscal, situacao, tipo_preco, email, zona, vendedor, vendedorId, morada, localidade, codigo_postal, isactive,
                telemovel, nif, data_upd, cond_Pagamento, tipo_entidade, pais, paisId, moeda, iban, nib, swift, modo_pagamento)
                SELECT E.Codigo, E.Nome, E.NomeFiscal, E.Situacao, E.TipoPrec, E.EnderecoWeb, E.Zona, E.Vendedor, E.VendedorId, E.Morada, E.Local, E.CP, E.IsActive, E.Tel,
                E.NumContrib, E.DataUltimaActualizacao, CP.Id, E.Tipo, E.Pais, P.ID, E.Moeda, E.IBAN, E.NIB, E.SWIFT, DB.Id
                FROM OPENQUERY([192.168.1.221, 58681\SVSQL\PRILEV100], 
                'select Entidades.* from 
                (
                	select C.Cliente as Codigo, C.Nome, C.NomeFiscal, case when C.ClienteAnulado = 1 then ''Inativo'' else ''Ativo'' end as Situacao, C.TipoPrec, C.EnderecoWeb, 
                	Z.Descricao as Zona, V.Nome as Vendedor, C.Vendedor as VendedorId, C.Fac_Mor as Morada, C.Fac_Local as Local, C.Fac_Cp as CP, case when C.ClienteAnulado = 1 then 0 else 1 end as IsActive,
                	C.Fac_Tel as Tel, C.NumContrib, C.DataUltimaActualizacao, C.CondPag, ''C'' as Tipo, C.Desconto, C.Pais, C.Moeda, CB.IBAN, CB.NIB, CB.SWIFT, C.ModoPag
                	from [{this.PriName}].[dbo].[Clientes] C
                	left join [{this.PriName}].[dbo].[Zonas] Z on Z.Zona = C.Zona
                	left join [{this.PriName}].[dbo].[Vendedores] V on V.Vendedor = C.Vendedor
                	left join [{this.PriName}].[dbo].[ContasBancarias] CB on CB.Entidade = C.Cliente and CB.TipoEntidade = ''C''
                
                	union all
                
                	select F.Fornecedor as Codigo, F.Nome, F.NomeFiscal, case when F.FornecedorAnulado = 1 then ''Inativo'' else ''Ativo'' end as Situacao, null, F.EnderecoWeb, 
                	null, null, null, F.Morada as Morada, F.Local as Local, F.Cp as CP, case when F.FornecedorAnulado = 1 then 0 else 1 end as IsActive,
                	F.Tel as Tel, F.NumContrib, F.DataUltimaActualizacao, F.CondPag, ''F'' as Tipo, 0, F.Pais, F.Moeda, CB.IBAN, CB.NIB, CB.SWIFT, F.ModoPag
                	from [{this.PriName}].[dbo].[Fornecedores] F
                	left join [{this.PriName}].[dbo].[ContasBancarias] CB on CB.Entidade = F.Fornecedor and CB.TipoEntidade = ''F''
                ) as Entidades') E 
                left join [{this.OurName}].[dbo].[CRM_CONDICOES_PAGAMENTO] CP on CP.Codigo = E.CondPag
                left join [{this.OurName}].[dbo].[CRM_PAISES] P on P.CODIGO = E.Pais
                left join [{this.OurName}].[dbo].[CRM_DOCUMENTOS_BANCOS] DB on DB.Movim = E.ModoPag

                COMMIT TRANSACTION;
            ";

            try
            {
                using (SqlConnection connection = new SqlConnection(connStr))
                {
                    connection.Open();

                    using (SqlCommand command = new SqlCommand(query, connection))
                    {
                        command.CommandTimeout = 0;
                        command.ExecuteNonQuery();
                    }

                    connection.Close();
                }

                var log = new DbImportationLog("CRM_ENTIDADE", "SUCCESS", "");
                log.Commit(connStr);
            }
            catch (Exception ex)
            {
                var log = new DbImportationLog("CRM_ENTIDADE", "FAILED", ex.Message);
                log.Commit(connStr);
            }
        }

        private void ImportArtigoMoedas(string connStr)
        {
            string query = $@"
                BEGIN TRANSACTION;

                TRUNCATE TABLE [{this.OurName}].[dbo].[CRM_ARTIGO_MOEDAS]
                
                INSERT INTO [{this.OurName}].[dbo].[CRM_ARTIGO_MOEDAS](ArtigoId, UnidadeId, Moeda, PVP1, PVP2, PVP3, PVP4, PVP5, PVP6)
                SELECT A.id, U.Id, AM.Moeda, AM.PVP1, AM.PVP2, AM.PVP3, AM.PVP4, AM.PVP5, AM.PVP6 
                FROM OPENQUERY([192.168.1.221, 58681\SVSQL\PRILEV100], 'select Artigo, Unidade, Moeda, PVP1, PVP2, PVP3, PVP4, PVP5, PVP6 from [{this.PriName}].[dbo].[ArtigoMoeda]') AM 
                inner join [{this.OurName}].[dbo].[CRM_ARTIGO] A on A.codigo_artigo = AM.Artigo
                inner join [{this.OurName}].[dbo].[CRM_UNIDADES] U on U.Un = AM.Unidade

                COMMIT TRANSACTION;
            ";

            try
            {
                using (SqlConnection connection = new SqlConnection(connStr))
                {
                    connection.Open();

                    using (SqlCommand command = new SqlCommand(query, connection))
                    {
                        command.CommandTimeout = 0;
                        command.ExecuteNonQuery();
                    }

                    connection.Close();
                }

                var log = new DbImportationLog("CRM_ARTIGO_MOEDAS", "SUCCESS", "");
                log.Commit(connStr);
            }
            catch (Exception ex)
            {
                var log = new DbImportationLog("CRM_ARTIGO_MOEDAS", "FAILED", ex.Message);
                log.Commit(connStr);
            }
        }

        private void ImportTiposDocumento(string connStr)
        {
            string query = $@"
                BEGIN TRANSACTION;

                TRUNCATE TABLE [{this.OurName}].[dbo].[CRM_TIPO_DOCUMENTO]

                INSERT INTO [{this.OurName}].[dbo].[CRM_TIPO_DOCUMENTO](NOME, MODULO, ENTRADASAIDA, MOVSTOCKS, CODIGOERP, ERPID, CLASS, ATIVO, CONTACORRENTE, PRECOSCOMIVA, CONTABILISTICO, INTEGRA_WT)
                SELECT TD.Descricao, TD.Modulo, TD.TipoDocSTK, TD.LigaStocks, TD.Documento as D, TD.Documento as DD, TD.ClasseAnalitica, 1, 0, 1, 0, 1
                FROM OPENQUERY([192.168.1.221, 58681\SVSQL\PRILEV100], '
                	select * 
                	from (
                		select Descricao, ''C'' as Modulo, TipoDocSTK, LigaStocks, Documento, ClasseAnalitica from [{this.PriName}].[dbo].[DocumentosCompra]
                	
                		union all
                	
                		select Descricao, ''V'' as Modulo, TipoDocSTK, LigaStocks, Documento, ClasseAnalitica from [{this.PriName}].[dbo].[DocumentosVenda]
                	
                		union all
                	
                		select Descricao, ''I'' as Modulo, TipoDocSTK, LigaStocks, Documento, ClasseAnalitica from [{this.PriName}].[dbo].[DocumentosInternos]
                	
                		union all
                	
                		select Descricao, ''T'' as Modulo, '''', 0, Documento, '''' from [{this.PriName}].[dbo].[DocumentosTesouraria]
                	) DT
                ') TD

                COMMIT TRANSACTION;
            ";

            try
            {
                using (SqlConnection connection = new SqlConnection(connStr))
                {
                    connection.Open();

                    using (SqlCommand command = new SqlCommand(query, connection))
                    {
                        command.CommandTimeout = 0;
                        command.ExecuteNonQuery();
                    }

                    connection.Close();
                }

                var log = new DbImportationLog("CRM_TIPO_DOCUMENTO", "SUCCESS", "");
                log.Commit(connStr);
            }
            catch (Exception ex)
            {
                var log = new DbImportationLog("CRM_TIPO_DOCUMENTO", "FAILED", ex.Message);
                log.Commit(connStr);
            }
        }

        private void ImportSeries(string connStr)
        {
            string query = $@"
                BEGIN TRANSACTION;

                TRUNCATE TABLE [{this.OurName}].[dbo].[CRM_SERIE_DOCUMENTO]
                
                INSERT INTO [{this.OurName}].[dbo].[CRM_SERIE_DOCUMENTO](ERPID, SERIE, TIPODOCUMENTOID, SETORERP, LOJAID, LAYOUTID)
                SELECT S.Serie as Se, Serie as SSe, TD.ID, S.Descricao, 0, ''
                FROM OPENQUERY([192.168.1.221, 58681\SVSQL\PRILEV100], '
                	select * from (
                		select Serie, TipoDoc, Descricao, ''C'' as Modulo from [{this.PriName}].[dbo].[SeriesCompras]
                	
                		union all
                	
                		select Serie, TipoDoc, Descricao, ''V'' as Modulo from [{this.PriName}].[dbo].[SeriesVendas]
                		
                		union all
                	
                		select Serie, TipoDoc, Descricao, ''I'' as Modulo from [{this.PriName}].[dbo].[SeriesInternos]
                		
                		union all
                	
                		select Serie, TipoDoc, Descricao, ''T'' as Modulo from [{this.PriName}].[dbo].[SeriesTesouraria]
                	) S
                ') S
                left join [{this.OurName}].[dbo].[CRM_TIPO_DOCUMENTO] TD on TD.CODIGOERP = S.TipoDoc and TD.MODULO = S.Modulo

                COMMIT TRANSACTION;
            ";

            try
            {
                using (SqlConnection connection = new SqlConnection(connStr))
                {
                    connection.Open();

                    using (SqlCommand command = new SqlCommand(query, connection))
                    {
                        command.CommandTimeout = 0;
                        command.ExecuteNonQuery();
                    }

                    connection.Close();
                }

                var log = new DbImportationLog("CRM_TIPO_DOCUMENTO", "SUCCESS", "");
                log.Commit(connStr);
            }
            catch (Exception ex)
            {
                var log = new DbImportationLog("CRM_TIPO_DOCUMENTO", "FAILED", ex.Message);
                log.Commit(connStr);
            }
        }

        private void ImportDocumentos(string connStr)
        {
            string query = $@"
                BEGIN TRANSACTION;

                TRUNCATE TABLE [{this.OurName}].[dbo].[CRM_DOCUMENTO]

                INSERT INTO [{this.OurName}].[dbo].[CRM_DOCUMENTO](NUMERO, DATA, VENCIMENTO, DESCONTO, T_IVA, T_LIQUIDO, T_ILIQUIDO, MODULO, OBSERVACOES, IDINTEGRACAO, FECHADO, NOME, MORADA, LOCALIDADE, NIF, ANULADO,
                TIPODOCUMENTOID, SERIEID, ENTIDADEID, CODIGOPOSTAL, PAISID, NUMDOCERP, CERTIFICADOAT, CONDPAGAMENTO, REFERENCIA, DATA_DESCARGA, DATA_CARGA, LOCAL_CARGA, LOCAL_DESCARGA,
                ESTADO, DELETED) 
                SELECT D.NumDoc, D.DataDoc, D.DataVencimento, D.Desconto, D.TotalIva, D.TLiquido, D.TILiquido, D.Modulo, D.Observacoes, D.Id, D.Fechado, D.Nome, D.Morada, D.Localidade, 
                D.NumContribuinte, D.Anulado, TD.ID, S.ID, E.id, D.CodPostal, P.ID, D.NumDocErp, D.Certificado, CP.Id, D.Referencia, D.DataDescarga, D.DataCarga, D.LocalCarga, D.LocalDescarga,
                D.Estado, D.Deleted
                FROM OPENQUERY([192.168.1.221, 58681\SVSQL\PRILEV100], '
                	select * 
                	from (
                		select CC.NumDoc, CC.DataDoc, CC.DataVencimento, CC.DescPag as Desconto, CC.TotalIva, (CC.TotalMerc + CC.TotalOutros - CC.TotalDesc) as TILiquido, 
                		(CC.TotalMerc + CC.TotalOutros + CC.TotalIva - CC.TotalDesc) as TLiquido, ''C'' as Modulo, CC.Observacoes, CC.Id, 1 as Fechado, CC.Nome, CC.Morada, CC.Localidade, CC.NumContribuinte, 
                		0 as Anulado, CC.TipoDoc, CC.Serie, CC.Entidade, CC.CodPostal, CC.Pais, CONVERT(nvarchar(200), CC.NumDoc) as NumDocErp, CC.Certificado, CC.CondPag, CC.Referencia, CC.DataDescarga, 
                		CC.DataCarga, CC.LocalCarga, CC.LocalDescarga, 8 as Estado, case when P.NumDoc is null then 0 else 1 end as Pendente, 0 as Deleted, CC.TipoEntidade 
                		from [{this.PriName}].[dbo].[CabecCompras] CC
                		left join [{this.PriName}].[dbo].[Pendentes] P on P.NumDoc = CONVERT(nvarchar(200), CC.NumDoc) and P.TipoDoc = CC.TipoDoc and P.Entidade = CC.Entidade
                	
                		union all
                	
                		select CD.NumDoc, CD.Data, CD.DataVencimento, CD.DescPag as Desconto, CD.TotalIva, (CD.TotalMerc + CD.TotalOutros - CD.TotalDesc) as TILiquido, 
                		(CD.TotalMerc + CD.TotalOutros + CD.TotalIva - CD.TotalDesc) as TLiquido, ''V'' as Modulo, CD.Observacoes, CD.Id, 1 as Fechado, CD.Nome, CD.Morada, CD.Localidade, CD.NumContribuinte, 
                		0 as Anulado, CD.TipoDoc, CD.Serie, CD.Entidade, CD.CodPostal, CD.Pais, CONVERT(nvarchar(200), CD.NumDoc) as NumDocErp, CD.Certificado, CD.CondPag, CD.Referencia, CD.DataDescarga, 
                		CD.DataCarga, CD.LocalCarga, CD.LocalDescarga, 8 as Estado, case when P.NumDoc is null then 0 else 1 end as Pendente, 0 as Deleted, CD.TipoEntidade 
                		from [{this.PriName}].[dbo].[CabecDoc] CD
                		left join [{this.PriName}].[dbo].[Pendentes] P on P.NumDoc = CONVERT(nvarchar(200), CD.NumDoc) and P.TipoDoc = CD.TipoDoc and P.Entidade = CD.Entidade
                		
                		union all
                	
                		select CI.NumDoc, CI.Data, CI.DataVencimento, CI.DescPag as Desconto, CI.TotalIva, (CI.TotalMerc - CI.TotalDesc) as TILiquido, (CI.TotalMerc + CI.TotalIva - CI.TotalDesc) as TLiquido, 
                		''I'' as Modulo, CI.Observacoes, CI.Id, 1 as Fechado, CI.Nome, CI.Morada, CI.Localidade, CI.NumContribuinte, 0 as Anulado, CI.TipoDoc, CI.Serie, CI.Entidade, CI.CodPostal, CI.Pais, 
                		CONVERT(nvarchar(200), CI.NumDoc) as NumDocErp, null, CI.CondPag, CI.Referencia, CI.DataDescarga, CI.DataCarga, CI.LocalCarga, CI.LocalDescarga, 8 as Estado, 
                		case when P.NumDoc is null then 0 else 1 end as Pendente, 0 as Deleted, CI.TipoEntidade 
                		from [{this.PriName}].[dbo].[CabecInternos] CI
                		left join [{this.PriName}].[dbo].[Pendentes] P on P.NumDoc = CONVERT(nvarchar(200), CI.NumDoc) and P.TipoDoc = CI.TipoDoc and P.Entidade = CI.Entidade
                		
                		union all
                	
                		select CT.NumDoc, CT.Data, null, null, CT.TotalIva, case when CT.TotalCredito <> 0 then CT.TotalCredito else CT.TotalDebito end as TILiquido, 
                		case when CT.TotalCredito <> 0 then (CT.TotalCredito + CT.TotalIva) else (CT.TotalDebito + CT.TotalIva) end as TLiquido, ''M'' as Modulo, CT.Observacoes, CT.Id, 
                		1 as Fechado, null, null, null, null, 0 as Anulado, CT.TipoDoc, CT.Serie, CT.Entidade, null, null, CONVERT(nvarchar(200), CT.NumDoc) as NumDocErp, null, null, null, null, null, null, 
                		null, 8 as Estado, case when P.NumDoc is null then 0 else 1 end as Pendente, 0 as Deleted, CT.TipoEntidade 
                		from [{this.PriName}].[dbo].[CabecTesouraria] CT
                		left join [{this.PriName}].[dbo].[Pendentes] P on P.NumDoc = CONVERT(nvarchar(200), CT.NumDoc) and P.TipoDoc = CT.TipoDoc and P.Entidade = CT.Entidade
                	) D
                ') D
                left join [{this.OurName}].[dbo].[CRM_TIPO_DOCUMENTO] TD on TD.CODIGOERP = D.TipoDoc and TD.MODULO = D.Modulo 
                left join [{this.OurName}].[dbo].[CRM_SERIE_DOCUMENTO] S on S.SERIE = D.Serie and S.TIPODOCUMENTOID = TD.ID
                left join [{this.OurName}].[dbo].[CRM_ENTIDADE] E on E.codigo = D.Entidade and E.tipo_entidade = D.TipoEntidade
                left join [{this.OurName}].[dbo].[CRM_PAISES] P on P.CODIGO = D.Pais
                left join [{this.OurName}].[dbo].[CRM_CONDICOES_PAGAMENTO] CP on CP.Codigo = D.CondPag

                COMMIT TRANSACTION;
            ";

            try
            {
                using (SqlConnection connection = new SqlConnection(connStr))
                {
                    connection.Open();

                    using (SqlCommand command = new SqlCommand(query, connection))
                    {
                        command.CommandTimeout = 0;
                        command.ExecuteNonQuery();
                    }

                    connection.Close();
                }

                var log = new DbImportationLog("CRM_DOCUMENTO", "SUCCESS", "");
                log.Commit(connStr);
            }
            catch (Exception ex)
            {
                var log = new DbImportationLog("CRM_DOCUMENTO", "FAILED", ex.Message);
                log.Commit(connStr);
            }
        }

        private void ImportDocumentosDetalhe(string connStr)
        {
            string query = $@"
                BEGIN TRANSACTION;

                TRUNCATE TABLE [{this.OurName}].[dbo].[CRM_DOCUMENTO_DETALHE]

                INSERT INTO [{this.OurName}].[dbo].[CRM_DOCUMENTO_DETALHE](ARTIGO, ARTIGO_ID, DESCRICAO, ORDEM, PRECOUNITARIO, DESCONTO, TAXAIVA, T_ILIQUDO, DOCUMENTOID, LOTE, unidade, ARMAZEM_DESTINO, ID_INTEGRACAO, QUANTIDADE)
                SELECT DD.Artigo, A.id, DD.Descricao, DD.NumLinha, DD.PrecUnit, DD.Desconto, DD.TaxaIva, DD.TotalIliquido, D.ID, DD.Lote,
                DD.Unidade, AR.ID, DD.Id, DD.Quantidade
                FROM OPENQUERY([192.168.1.221, 58681\SVSQL\PRILEV100], '
                	select * from (
                		select Artigo, Descricao, NumLinha, PrecUnit, Quantidade, (ISNULL(Desconto1, 0) + ISNULL(Desconto2, 0) + ISNULL(Desconto3, 0)) as Desconto, 
                		TaxaIva, TotalIliquido, IdCabecDoc as CabecId, Lote, Unidade, Armazem, Id
                		from [{this.PriName}].[dbo].[LinhasDoc]
                	
                		union all
                	
                		select Artigo, Descricao, NumLinha, PrecUnit, Quantidade, (ISNULL(Desconto1, 0) + ISNULL(Desconto2, 0) + ISNULL(Desconto3, 0)) as Desconto, 
                		TaxaIva, TotalIliquido, IdCabecCompras as CabecId, Lote, Unidade, Armazem, Id
                		from [{this.PriName}].[dbo].[LinhasCompras]
                		
                		union all
                	
                		select Artigo, Descricao, NumLinha, PrecUnit, Quantidade, (ISNULL(Desconto1, 0) + ISNULL(Desconto2, 0) + ISNULL(Desconto3, 0)) as Desconto, 
                		TaxaIva, TotalIliquido, IdCabecInternos as CabecId, Lote, Unidade, Armazem, Id
                		from [{this.PriName}].[dbo].[LinhasInternos]
                		
                		union all
                	
                		select null, Descricao, Linha, case when Credito <> 0 then Credito else Debito end as PrecUnit, 1, 0 as Desconto, Iva as TaxaIva, 
                		case when Credito <> 0 then Credito else Debito end as TotalIliquido, IdCabecTesouraria as CabecId, null, null, null, Id
                		from [{this.PriName}].[dbo].[LinhasTesouraria]
                	) DD
                ') DD
                left join [{this.OurName}].[dbo].[CRM_ARTIGO] A on A.codigo_artigo = DD.Artigo
                left join [{this.OurName}].[dbo].[CRM_DOCUMENTO] D on D.IDINTEGRACAO = DD.CabecId
                left join [{this.OurName}].[dbo].[CRM_ARMAZEM] AR on AR.CODIGOERP = DD.Armazem

                COMMIT TRANSACTION;
            ";

            try
            {
                using (SqlConnection connection = new SqlConnection(connStr))
                {
                    connection.Open();

                    using (SqlCommand command = new SqlCommand(query, connection))
                    {
                        command.CommandTimeout = 0;
                        command.ExecuteNonQuery();
                    }

                    connection.Close();
                }

                var log = new DbImportationLog("CRM_DOCUMENTO_DETALHE", "SUCCESS", "");
                log.Commit(connStr);
            }
            catch (Exception ex)
            {
                var log = new DbImportationLog("CRM_DOCUMENTO_DETALHE", "FAILED", ex.Message);
                log.Commit(connStr);
            }
        }
    }
}
