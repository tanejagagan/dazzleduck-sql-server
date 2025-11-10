import { describe, it, expect, beforeAll } from 'vitest';
import { renderHook } from '@testing-library/react';
import { LoggingProvider, useLogging } from '../src/context/LoggingContext';
import Cookies from 'js-cookie';

const SERVER_URL = 'http://localhost:8080';
const USERNAME = 'admin';
const PASSWORD = 'admin';

const renderUseLogging = () =>
    renderHook(() => useLogging(), {
        wrapper: ({ children }) => <LoggingProvider>{children}</LoggingProvider>,
    });

describe('LoggingContext Integration Tests', () => {
    let jwtToken;
    let result;

    beforeAll(async () => {
        const hook = renderUseLogging();
        result = hook.result;
        jwtToken = await result.current.login(SERVER_URL, USERNAME, PASSWORD);
        console.log('JWT Token: ', jwtToken);
        Cookies.set('jwtToken', jwtToken);
    });

    // Basic test to ensure login works by checking if a token is string
    it('should login successfully and return a token', () => {
        expect(jwtToken).toBeDefined();
        console.log(jwtToken, " <<----jwt-----")
        expect(typeof jwtToken).toBe('string');
    });

    // /query tests ---------------------- START
    it('should runQuery directly (split size 0)', async () => {
        const directRes = await result.current.executeQuery(
            SERVER_URL,
            'select 2+2 as sum',
            0,
            jwtToken
        );

        expect(Array.isArray(directRes)).toBe(true);
        if (directRes.length > 0) {
            expect(directRes[0]).toHaveProperty('sum');
            expect(directRes[0].sum).toBe(4);
        }
    });

    it('should execute a simple select query', async () => {
        const rows = await result.current.executeQuery(
            SERVER_URL,
            'select 1 as one',
            0,
            jwtToken
        );

        expect(Array.isArray(rows)).toBe(true);
        if (rows.length > 0) {
            expect(rows[0]).toHaveProperty('one');
            expect(rows[0].one).toBe(1);
        }
    });

    it('should execute the simple /query with alias', async () => {
        const rows = await result.current.executeQuery(
            SERVER_URL,
            'select 21 as age',
            0,
            jwtToken
        );

        expect(Array.isArray(rows)).toBe(true);
        if (rows.length > 0) {
            expect(rows[0]).toHaveProperty('age');
            expect(rows[0].age).toBe(21);
        }
    });
    // /query tests ---------------------- END

    // /plan and split queries tests ---------------------- START
    it('should execute /plan -> /query split logic correctly', async () => {
        const splitQuery = `FROM (FROM (VALUES(NULL::DATE, NULL::VARCHAR, NULL::VARCHAR, NULL::VARCHAR)) t(dt, p, key, value)
        WHERE false
        UNION ALL BY NAME
        FROM read_parquet('example/hive_table/*/*/*.parquet', hive_partitioning = true, hive_types = {'dt': DATE, 'p': VARCHAR}))`;

        const resultRows = await result.current.executeQuery(
            SERVER_URL,
            splitQuery,
            1,
            jwtToken
        );

        expect(Array.isArray(resultRows)).toBe(true);
        if (resultRows.length > 0) {
            const row = resultRows[0];
            expect(row).toHaveProperty('dt');
            expect(row).toHaveProperty('p');
            expect(row).toHaveProperty('key');
            expect(row).toHaveProperty('value');
        }
    });
    // /plan and split queries tests ---------------------- END

    // Invalid input handling
    it('should handle invalid input gracefully', async () => {
        await expect(
            result.current.executeQuery(SERVER_URL, '', 0, jwtToken)
        ).rejects.toThrow(/Please fill in all fields/);
    });

    // Handle /plan returning no splits
    it('should handle /plan returning no splits gracefully', async () => {
        const badSplitQuery = 'select * from wrong_table';
        try {
            await result.current.executeQuery(SERVER_URL, badSplitQuery, 1, jwtToken);
        } catch (err) {
            expect(err.message).toMatch(/Query execution failed/);
        }
    });

    // Invalid token test (skipped for now)
    it.skip('should fail if token is invalid', async () => {
        Cookies.set('jwtToken', 'invalidtoken');
        await expect(
            result.current.executeQuery(SERVER_URL, 'select 1', 0, 'invalidtoken')
        ).rejects.toThrow(/Fail|responded|Unauthorized/i);
    });

    // Forwarding test
    it('should forward to a valid endpoint using forwardToDazzleDuck internally', async () => {
        const res = await result.current.executeQuery(
            SERVER_URL,
            'select current_date',
            0,
            jwtToken
        );
        expect(Array.isArray(res)).toBe(true);
    });
});
