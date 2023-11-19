package trocaBancoPostgres.migrar;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

import trocaBancoPostgres.conexao.ConexaoAntiga;
import trocaBancoPostgres.conexao.ConexaoNova;

public class Cliente {

	public static void migrarTabela() {
		try (Connection remetenteConexao = ConexaoAntiga.obterConexao()) {

			String query = "SELECT ecft_id, sis_ecft, ecft_tipo, ecft_nome, ecft_cnpj, "
					+ " ecft_inscricao, ecft_descricao, ecft_telefone, ecft_endereco, ecft_no, ecft_cep,"
					+ " ecft_complemento, ecft_bairro, ecft_cidade, ecft_observacao, ecft_usuario,"
					+ " ecft_registro, stecft\n" + "FROM public.ecft; ";

			try (Statement sourceStatement = remetenteConexao.createStatement();
					ResultSet resultSet = sourceStatement.executeQuery(query)) {

				while (resultSet.next()) {
					int id = resultSet.getInt("sis_ecft");
					String nome = resultSet.getString("ecft_nome");
					String cnpj = resultSet.getString("ecft_cnpj");
					String insc = resultSet.getString("ecft_inscricao");
					String descricao = resultSet.getString("ecft_descricao");
					String telefone = resultSet.getString("ecft_telefone");
					String endereco = resultSet.getString("ecft_endereco");
					String numero = resultSet.getString("ecft_no");
					String cep = resultSet.getString("ecft_cep");
					String complemento = resultSet.getString("ecft_complemento");
					String bairro = resultSet.getString("ecft_bairro");
					String cidade = resultSet.getString("ecft_cidade");
					String observacao = resultSet.getString("ecft_observacao");

					System.out.println("" + descricao);
					salvar(id, cnpj, nome, descricao, insc, endereco);
				}
			}
		} catch (SQLException e) {
			e.printStackTrace();
		}
	}

	public static void salvar(int idAntigo, String cnpj, String nome, String desc, String insc, String end) {
		try (Connection novaConexao = ConexaoNova.obterConexao()) {
			String sql = "INSERT INTO cliente (tipo_cliente, " + "cnpj, " + "razao_social," + " nome_fantasia, "
					+ "  inscricao_estadual," + " inscricao_municipal," + " endereco, " + "contato,  "
					+ "  responsavel_legal, " + "tipo_empresa, " + "ativo,idAntigo)  \n"
					+ "   VALUES (1, ?, ?, ?, ?, '', ?, '', '', 1, true,?)";

			try (PreparedStatement preparedStatement = novaConexao.prepareStatement(sql)) {

				preparedStatement.setString(1, cnpj);
				preparedStatement.setString(2, nome);
				preparedStatement.setString(3, desc);
				preparedStatement.setString(4, insc);
				preparedStatement.setString(5, end);

				preparedStatement.setInt(6, idAntigo);

				preparedStatement.executeUpdate();
			}

		} catch (SQLException e) {
			e.printStackTrace();
		}
	}

	public static void dropTable(String tableName) {
		try (Connection connection = ConexaoNova.obterConexao()) {
			Statement statement = connection.createStatement();

			String sql = "DROP TABLE " + tableName + " CASCADE";
			statement.executeUpdate(sql);

			System.out.println("Tabela " + tableName + " excluída com sucesso.");

		} catch (SQLException e) {
			e.printStackTrace();
		}
	}

	public static void createTable() {
		try (Connection connection = ConexaoNova.obterConexao()) {
			Statement statement = connection.createStatement();

			String sql = " CREATE TABLE cliente (\n" + "    id SERIAL PRIMARY KEY,\n"
					+ "    tipo_cliente INTEGER NOT NULL,\n" + "    cnpj VARCHAR(18) NOT NULL,\n"
					+ "    razao_social VARCHAR(255) NOT NULL,\n" + "    nome_fantasia VARCHAR(255),\n"
					+ "    inscricao_estadual VARCHAR(20),\n" + "    inscricao_municipal VARCHAR(20),\n"
					+ "    endereco VARCHAR(255) NOT NULL,\n" + "    contato VARCHAR(255) ,\n"
					+ "    responsavel_legal VARCHAR(255),\n" + "    tipo_empresa INTEGER NOT NULL,\n"
					+ "    deletado BOOLEAN NOT NULL DEFAULT false \n" + ");";

			statement.executeUpdate(sql);

			System.out.println("Tabela Cliente criada com sucesso.");

		} catch (SQLException e) {
			e.printStackTrace();
		}
	}

	public static void adicionarColunaDeletado() {
		try (Connection connection = ConexaoNova.obterConexao()) {
			Statement statement = connection.createStatement();

			String sql = "ALTER TABLE Cliente\n " + "ALTER COLUMN deletado SET DEFAULT false;";
			statement.executeUpdate(sql);

			System.out.println("Coluna 'deletado' adicionada à tabela 'Cliente' com sucesso.");

		} catch (SQLException e) {
			e.printStackTrace();
		}
	}

	public static void adicionarColunaIdAntigo() {
		try (Connection connection = ConexaoNova.obterConexao()) {
			Statement statement = connection.createStatement();

			String sql = "ALTER TABLE Cliente ADD idAntigo int";
			statement.executeUpdate(sql);

			System.out.println("Coluna 'idAntigo' adicionada à tabela 'Cliente' com sucesso.");

		} catch (SQLException e) {
			e.printStackTrace();
		}
	}

	public static void adicionarColunaAtivo() {
		try (Connection connection = ConexaoNova.obterConexao()) {
			Statement statement = connection.createStatement();

			String sql = "ALTER TABLE cliente\n" + "ADD COLUMN ativo BOOLEAN DEFAULT TRUE;";
			statement.executeUpdate(sql);

			System.out.println("Coluna 'deletado' adicionada à tabela 'NCM' com sucesso.");

		} catch (SQLException e) {
			e.printStackTrace();
		}
	}

//	public static int encontraCliente(int idAntingo) throws SQLException {
//		Connection conn = ConexaoNova.obterConexao();
//		int id = 0;
//		try {
//			String sql = "SELECT id, sigla, descricao, ativo FROM Cliente WHERE idAntigo = ?";
//			PreparedStatement preparedStatement = conn.prepareStatement(sql);
//			preparedStatement.setInt(1, idAntingo);
//			ResultSet resultSet = preparedStatement.executeQuery();
//			if (resultSet.next()) {
//				id = resultSet.getInt("id");
//			}
//		} catch (SQLException e) {
//			e.printStackTrace();
//		}
//		conn.close();
//		return id;
//
//	}

	public static void main(String[] args) {
		dropTable("Cliente");
		createTable();
		adicionarColunaDeletado();
		adicionarColunaIdAntigo();
		adicionarColunaAtivo();
		migrarTabela();
	}
}